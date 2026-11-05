/**
* @NApiVersion 2.1
* @NScriptType MapReduceScript
*/
define(['N/search', 'N/file', 'N/record', 'N/log', 'N/email', 'N/runtime'],
(search, file, record, log, email, runtime) => {
  
  const FOLDER_ID = 1943095;
  const PROCESSED_FOLDER_ID = 1943096;
  const ERROR_FOLDER_ID = 0; // TODO: set Error folder internal id
  const RECORD_TYPE = 'customrecord_ft_walmart_grs';
  const LOCATION_MASTER_TYPE = 'customrecord_ft_location_master';
  const RETAILER_POOL_WALMART = '1';
  
  // Walmart Pool (CSV column 1) -> NetSuite Location internal id
  const POOL_LOCATION_MAP = {
    CA: '32',
    CA2: '32',
    GA: '48',
    GA2: '48',
    IL: '2',
    NJ: '1',
    TX: '51',
    T2: '51'
  };
  
  function getPoolLocation(pool) {
    return POOL_LOCATION_MAP[String(pool || '').trim().toUpperCase()] || '';
  }
  
  const EMAIL_AUTHOR = 902578;     // TODO: employee internal id
  const EMAIL_RECIPIENTS = [902578]; // TODO: recipients
  const MAX_ERRORS_IN_BODY = 50;   // full list goes in the CSV attachment
  
  // Normalize DC numbers so "06094" and "6094" match
  function normDC(val) {
    return String(val || '').trim().replace(/^0+/, '');
  }
  
  // ---------------------------------------------------------------------------
  // GET INPUT DATA - read files, build rows. Files are NOT moved here.
  // ---------------------------------------------------------------------------
  function getInputData() {
    const returnArray = [];
    
    try {
      // Walmart Pool mapping
      const mappingCache = {};
      search.create({
        type: 'customrecord_ft_walmart_grs_mapping',
        filters: [['isinactive', 'is', 'F']],
        columns: [
          'custrecord_ft_wmgrsmap_externalid',
          'custrecord_ft_wmgrsmap_location',
          'custrecord_ft_wmgrsmap_weeks',
          'internalid'
        ]
      }).run().each(result => {
        mappingCache[result.getValue('custrecord_ft_wmgrsmap_externalid')] = {
          location: result.getValue('custrecord_ft_wmgrsmap_location'),
          weeks: result.getValue('custrecord_ft_wmgrsmap_weeks'),
          internalid: result.getValue('internalid')
        };
        return true;
      });
      
      // Walmart DC lookup (Location Master)
      const dcCache = {};
      search.create({
        type: LOCATION_MASTER_TYPE,
        filters: [
          ['isinactive', 'is', 'F'], 'AND',
          ['custrecord_ft_consloc_retailer_pool', 'anyof', RETAILER_POOL_WALMART], 'AND',
          ['custrecord_ft_wmdc_number', 'isnotempty', '']
        ],
        columns: ['internalid', 'custrecord_ft_wmdc_number']
      }).run().each(result => {
        const dcKey = normDC(result.getValue('custrecord_ft_wmdc_number'));
        if (dcKey && !dcCache[dcKey]) dcCache[dcKey] = result.getValue('internalid');
        return true;
      });
      
      // Files in inbound folder
      search.create({
        type: 'file',
        filters: [['folder', 'anyof', FOLDER_ID]],
        columns: ['internalid', 'name', 'created']
      }).run().each(result => {
        const fileId = result.getValue('internalid');
        const fileName = result.getValue('name');
        const created = result.getValue('created');
        
        // Meta row: guarantees every file reaches reduce, even with 0 data rows
        returnArray.push({ type: 'meta', fileId, fileName, created });
        
        try {
          const content = file.load({ id: fileId }).getContents();
          const lines = content.split(/\r?\n/);
          let isUpdate = false;
          
          lines.forEach((line, idx) => {
            if (line.trim() === '') return;
            
            const cols = splitCSV(line);
            const lastCol = cols[cols.length - 1];
            if (!isUpdate) isUpdate = lastCol == 'Internal ID';
            if (lastCol == 'Internal ID') return; // header of update file
            
            const base = { type: 'row', fileId, fileName, lineNo: idx + 1, raw: line };
            
            if (isUpdate) {
              base.internalid = lastCol;
              returnArray.push(base);
              return;
            }
            
            const vendor = cols[4];
            const pool = cols[0];
            const dc = cols[1];
            const keysToTry = [vendor, pool + '_' + vendor, dc + '_' + vendor, pool];
            
            let matched = { location: '', weeks: '', internalid: '' };
            for (let i = 0; i < keysToTry.length; i++) {
              if (mappingCache[keysToTry[i]]) {
                matched = mappingCache[keysToTry[i]];
                break;
              }
            }
            
            base.data = {
              custrecord_ft_pool: cols[0],
              custrecord_ft_pool_id: matched.internalid || '',
              custrecord_ft_dc: cols[1],
              custrecord_ft_dc_id: dcCache[normDC(dc)] || '',
              custrecord_ft_truck: cols[2],
              name: cols[3],
              custrecord_ft_vendor_number: cols[4],
              custrecord_ft_vendor: cols[5],
              custrecord_ft_department: cols[6],
              custrecord_ft_item_number: cols[7],
              custrecord_ft_item_description: cols[8],
              custrecord_ft_order_quantity: cols[9],
              custrecord_ft_weight_quantity: cols[10],
              custrecord_ft_cube_quantity: cols[11],
              custrecord_ft_mabd: cols[12],
              custrecord_ft_purchase_order_type: cols[13],
              mapping: matched
            };
            returnArray.push(base);
          });
        } catch (fileErr) {
          // Whole file could not be read/parsed -> goes to Error folder in reduce
          log.error('File read error', fileName + ' (' + fileId + '): ' + fileErr.message);
          returnArray.push({ type: 'fileError', fileId, fileName, message: fileErr.message });
        }
        
        return true;
      });
      
      return returnArray;
    } catch (err) {
      log.error('getInputData Error', err.message);
      return [];
    }
  }
  
  // ---------------------------------------------------------------------------
  // MAP - process one row, write result keyed by file id
  // ---------------------------------------------------------------------------
  function map(context) {
    const row = JSON.parse(context.value);
    
    if (row.type === 'meta') {
      context.write({ key: row.fileId, value: row });
      return;
    }
    
    if (row.type === 'fileError') {
      context.write({
        key: row.fileId,
        value: { type: 'result', status: 'error', lineNo: 0, message: 'File could not be read: ' + row.message, raw: '' }
      });
      return;
    }
    
    try {
      let recId;
      
      if (row.internalid) {
        recId = record.submitFields({
          type: RECORD_TYPE,
          id: row.internalid,
          values: { custrecord_ft_grs_confirmed: true }
        });
      } else {
        recId = createGrsRecord(row.data);
      }
      
      context.write({
        key: row.fileId,
        value: { type: 'result', status: 'success', lineNo: row.lineNo, recordId: recId }
      });
    } catch (e) {
      log.error('map error', row.fileName + ' line ' + row.lineNo + ': ' + e.message);
      context.write({
        key: row.fileId,
        value: { type: 'result', status: 'error', lineNo: row.lineNo, message: e.message, raw: row.raw }
      });
    }
  }
  
  function createGrsRecord(row) {
    // Treat missing lookups as errors so the file is flagged
    if (!row.custrecord_ft_pool_id) {
      throw new Error('Pool mapping not found (Pool: ' + row.custrecord_ft_pool + ', Vendor: ' + row.custrecord_ft_vendor_number + ')');
    }
    if (!row.custrecord_ft_dc_id) {
      throw new Error('DC not found in Location Master (DC: ' + row.custrecord_ft_dc + ')');
    }
    
    const rec = record.create({ type: RECORD_TYPE, isDynamic: true });
    
    rec.setValue('custrecord_ft_pool', row.custrecord_ft_pool_id);
    rec.setValue('custrecord_ft_dc', row.custrecord_ft_dc_id);
    rec.setValue('custrecord_ft_truck', row.custrecord_ft_truck);
    rec.setValue('name', row.name);
    rec.setValue('custrecord_ft_vendor_number', row.custrecord_ft_vendor_number);
    rec.setValue('custrecord_ft_vendor', row.custrecord_ft_vendor);
    rec.setValue('custrecord_ft_department', row.custrecord_ft_department);
    rec.setValue('custrecord_ft_item_number', row.custrecord_ft_item_number);
    rec.setValue('custrecord_ft_item_description', row.custrecord_ft_item_description);
    rec.setValue('custrecord_ft_order_quantity', parseFloat(row.custrecord_ft_order_quantity));
    rec.setValue('custrecord_ft_weight_quantity', parseFloat(row.custrecord_ft_weight_quantity));
    rec.setValue('custrecord_ft_cube_quantity', parseFloat(row.custrecord_ft_cube_quantity));
    rec.setValue('custrecord_ft_purchase_order_type', row.custrecord_ft_purchase_order_type);
    
    if (row.custrecord_ft_mabd) {
      const p = row.custrecord_ft_mabd.split('-');
      const d = new Date(p[0], p[1] - 1, p[2]);
      if (isNaN(d.getTime())) throw new Error('Invalid MABD date: ' + row.custrecord_ft_mabd);
      rec.setValue('custrecord_ft_mabd', d);
      
      const adj = getAdjustedFriday(row.custrecord_ft_mabd);
      if (adj) rec.setValue('custrecord_ft_adj_mabd', adj);
    }
    
    // Location from Pool mapping; fall back to Walmart Pool mapping record
    const locationId = getPoolLocation(row.mapping.location) || row.mapping.location || '';
    if (!locationId) {
      throw new Error('No location mapping for Pool: ' + row.custrecord_ft_pool);
    }
    rec.setValue('custrecord_ft_grs_location', locationId);
    rec.setValue('custrecord_ft_twovsthree_week_category', row.mapping.weeks || '');
    rec.setValue('custrecord_ft_walmart_grs_mapping', row.mapping.internalid || '');

    var adjMabd = rec.getValue('custrecord_ft_adj_mabd');

    rec.setValue('externalid',
         row.custrecord_ft_vendor_number + '_' +
         row.name + '_' +
         row.custrecord_ft_item_number + '_' +
         (adjMabd
             ? adjMabd.getFullYear() + '-' +
               String(adjMabd.getMonth() + 1).padStart(2, '0') + '-' +
               String(adjMabd.getDate()).padStart(2, '0')
             : '')
    );
    
    return rec.save();
  }
  
  // ---------------------------------------------------------------------------
  // REDUCE - one call per file: tally results, move file to Processed or Error
  // ---------------------------------------------------------------------------
  function reduce(context) {
    const fileId = context.key;
    let meta = { fileName: '', created: '' };
    let successCount = 0;
    const errors = [];
    
    context.values.forEach(v => {
      const val = JSON.parse(v);
      if (val.type === 'meta') {
        meta = val;
      } else if (val.status === 'success') {
        successCount++;
      } else {
        errors.push({ lineNo: val.lineNo, message: val.message, raw: val.raw });
      }
    });
    
    const hasErrors = errors.length > 0;
    let movedTo = hasErrors ? 'Error' : 'Processed';
    
    try {
      const f = file.load({ id: fileId });
      f.folder = hasErrors ? ERROR_FOLDER_ID : PROCESSED_FOLDER_ID;
      f.save();
    } catch (moveErr) {
      log.error('File move failed', fileId + ': ' + moveErr.message);
      movedTo = 'NOT MOVED (' + moveErr.message + ')';
    }
    
    errors.sort((a, b) => a.lineNo - b.lineNo);
    
    context.write({
      key: fileId,
      value: {
        fileId,
        fileName: meta.fileName,
        created: meta.created,
        status: hasErrors ? 'Error' : 'Success',
        successCount,
        errorCount: errors.length,
        movedTo,
        errors
      }
    });
  }
  
  // ---------------------------------------------------------------------------
  // SUMMARIZE - send one email with per-file success/error + error CSVs
  // ---------------------------------------------------------------------------
  function summarize(summary) {
    if (summary.inputSummary.error) {
      log.error('Input stage error', summary.inputSummary.error);
    }
    summary.mapSummary.errors.iterator().each((key, err) => {
      log.error('Uncaught map error', key + ': ' + err);
      return true;
    });
    summary.reduceSummary.errors.iterator().each((key, err) => {
      log.error('Uncaught reduce error', 'File ' + key + ': ' + err);
      return true;
    });
    
    const results = [];
    summary.output.iterator().each((key, value) => {
      results.push(JSON.parse(value));
      return true;
    });
    
    if (!results.length) {
      log.audit('No files processed', 'Nothing in folder ' + FOLDER_ID);
      return;
    }
    
    sendResultEmail(results);
  }
  
  function sendResultEmail(results) {
    const td = 'border:1px solid #ccc; padding:6px;';
    const errorFiles = results.filter(r => r.errorCount > 0);
    
    const summaryRows = results.map((r, idx) => {
      const color = r.errorCount > 0 ? '#c0392b' : '#27ae60';
      return '<tr>' +
        '<td style="' + td + ' text-align:center;">' + (idx + 1) + '</td>' +
        '<td style="' + td + '">' + esc(r.fileName) + '</td>' +
        '<td style="' + td + '">' + esc(r.created) + '</td>' +
        '<td style="' + td + '">' + r.fileId + '</td>' +
        '<td style="' + td + ' color:' + color + '; font-weight:bold;">' + r.status + '</td>' +
        '<td style="' + td + ' text-align:center;">' + r.successCount + '</td>' +
        '<td style="' + td + ' text-align:center;">' + r.errorCount + '</td>' +
        '<td style="' + td + '">' + esc(r.movedTo) + '</td>' +
        '</tr>';
    }).join('');
    
    let body = 'Hi Team,<br/><br/>Walmart GRS file processing results:<br/><br/>' +
      '<table style="border-collapse:collapse; width:100%; font-family:Arial; font-size:12px;">' +
      '<thead><tr style="background:#f2f2f2;">' +
      ['#', 'File Name', 'Date', 'File ID', 'Status', 'Success', 'Errors', 'Moved To']
        .map(h => '<th style="' + td + '">' + h + '</th>').join('') +
      '</tr></thead><tbody>' + summaryRows + '</tbody></table>';
    
    const attachments = [];
    
    errorFiles.forEach(r => {
      body += '<br/><br/><b>Errors - ' + esc(r.fileName) + '</b>' +
        '<table style="border-collapse:collapse; width:100%; font-family:Arial; font-size:12px;">' +
        '<thead><tr style="background:#f9e0e0;">' +
        '<th style="' + td + '">Line</th><th style="' + td + '">Error</th>' +
        '</tr></thead><tbody>' +
        r.errors.slice(0, MAX_ERRORS_IN_BODY).map(e =>
          '<tr><td style="' + td + ' text-align:center;">' + (e.lineNo || '-') + '</td>' +
          '<td style="' + td + '">' + esc(e.message) + '</td></tr>'
        ).join('') +
        '</tbody></table>';
      
      if (r.errors.length > MAX_ERRORS_IN_BODY) {
        body += '<i>Showing first ' + MAX_ERRORS_IN_BODY + ' of ' + r.errors.length + ' errors. See attachment for full list.</i>';
      }
      
      // Error CSV attachment (not saved to File Cabinet)
      const csv = ['Line No,Error,Original Line']
        .concat(r.errors.map(e => [e.lineNo, csvCell(e.message), csvCell(e.raw)].join(',')))
        .join('\n');
      
      attachments.push(file.create({
        name: 'ERRORS_' + String(r.fileName || r.fileId).replace(/\.csv$/i, '') + '.csv',
        fileType: file.Type.CSV,
        contents: csv
      }));
    });
    
    body += '<br/><br/>Files with errors have been moved to the Error folder. ' +
      'Successful rows were created; fix and re-submit only the failed lines.' +
      '<br/><br/>Regards,<br/>Fusion Transport, LLC';
    
    const subject = 'Walmart GRS Files Processed - ' +
      (results.length - errorFiles.length) + ' Success, ' + errorFiles.length + ' Error';
    
    email.send({
      author: EMAIL_AUTHOR,
      recipients: EMAIL_RECIPIENTS,
      subject: subject,
      body: body,
      attachments: attachments.slice(0, 10)
    });
    
    log.audit('Email Sent', subject);
  }
  
  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  function splitCSV(str) {
    const result = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < str.length; i++) {
      const char = str[i];
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  }
  
  function getAdjustedFriday(date) {
    date = date.split('-');
    const d = new Date(date[0], date[1] - 1, date[2]);
    const dow = d.getDay(); // 0=Sun ... 5=Fri ... 6=Sat
    if (dow === 5) return d;
    
    // Sat/Sun/Mon/Tue -> prior Friday
    if (dow === 6 || dow === 0 || dow === 1 || dow === 2) {
      const back = ((dow - 5 + 7) % 7) || 7;
      d.setDate(d.getDate() - back);
      return d;
    }
    
    // Wed/Thu -> next Friday
    const forward = (5 - dow + 7) % 7;
    d.setDate(d.getDate() + (forward === 0 ? 7 : forward));
    return d;
  }
  
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  
  function csvCell(s) {
    return '"' + String(s == null ? '' : s).replace(/"/g, '""') + '"';
  }
  
  return {
    getInputData,
    map,
    reduce,
    summarize
  };
}
);