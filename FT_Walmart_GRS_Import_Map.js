/**
* @NApiVersion 2.1
* @NScriptType MapReduceScript
*/
define(['N/search', 'N/file', 'N/record', 'N/log', 'N/email', 'N/runtime'],
(search, file, record, log, email, runtime) => {

  // ---------------------------------------------------------------------------
  // DRY RUN SWITCH
  // true  = nothing is created/updated, files are NOT moved, results only logged
  // false = LIVE: create/update records, move files, send email
  // ---------------------------------------------------------------------------
  const DRY_RUN = false;
  const SEND_EMAIL_IN_DRY_RUN = false; // set true if you also want the email during dry run

  const FOLDER_ID = 1943095;
  const PROCESSED_FOLDER_ID = 1943096;
  const ERROR_FOLDER_ID = 2814808; 
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

  // Field types used when comparing existing vs new values
  const DATE_FIELDS = ['custrecord_ft_mabd', 'custrecord_ft_adj_mabd'];
  const NUMBER_FIELDS = ['custrecord_ft_order_quantity', 'custrecord_ft_weight_quantity', 'custrecord_ft_cube_quantity'];
  // List fields whose IDs differ between the mapping and the GRS record -> compare/update by display text
  const TEXT_COMPARE_FIELDS = ['custrecord_ft_twovsthree_week_category'];

  // Normalize DC numbers so "06094" and "6094" match
  function normDC(val) {
    return String(val || '').trim().replace(/^0+/, '');
  }

  // ---------------------------------------------------------------------------
  // GET INPUT DATA - read files, build rows. Files are NOT moved here.
  // ---------------------------------------------------------------------------
  function getInputData() {
    const returnArray = [];

    if (DRY_RUN) log.audit('*** DRY RUN MODE ***', 'No records will be created/updated and no files will be moved.');

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
          weeksText: result.getText('custrecord_ft_wmgrsmap_weeks') || result.getValue('custrecord_ft_wmgrsmap_weeks'),
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

    const where = row.fileName + ' line ' + row.lineNo;

    try {
      let result;

      if (row.internalid) {
        // Confirmation file (has Internal ID column)
        if (DRY_RUN) {
          log.audit('DRY RUN - WOULD CONFIRM', where + ': record ' + row.internalid);
        } else {
          record.submitFields({
            type: RECORD_TYPE,
            id: row.internalid,
            values: { custrecord_ft_grs_confirmed: true }
          });
        }
        result = { action: 'confirm', recordId: row.internalid };
      } else {
        result = upsertGrsRecord(row.data);
       // logUpsert(where, result);
      }

      context.write({
        key: row.fileId,
        value: { type: 'result', status: 'success', action: result.action, lineNo: row.lineNo, recordId: result.recordId }
      });
    } catch (e) {
     // log.error((DRY_RUN ? 'DRY RUN - ' : '') + 'map error', where + ': ' + e.message);
      context.write({
        key: row.fileId,
        value: { type: 'result', status: 'error', lineNo: row.lineNo, message: e.message, raw: row.raw }
      });
    }
  }

  // ---------------------------------------------------------------------------
  // ADD OR UPDATE by External ID
  //   not found      -> create
  //   found, changed -> update only the changed fields
  //   found, same    -> skip
  // ---------------------------------------------------------------------------
  function upsertGrsRecord(row) {
    const built = buildGrsValues(row); // validates + builds values and external id
    const extId = built.extId;
    const values = built.values;

    const existingId = findByExternalId(extId);

    // ---- CREATE ----
    if (!existingId) {
      if (DRY_RUN) return { action: 'create', recordId: null, extId };

      const rec = record.create({ type: RECORD_TYPE, isDynamic: true });
      Object.keys(values).forEach(f => {
        if (values[f] !== null && values[f] !== '') rec.setValue(f, values[f]);
      });
      rec.setValue('externalid', extId);
      return { action: 'create', recordId: rec.save(), extId };
    }

    // ---- UPDATE (only if something changed) ----
    const rec = record.load({ type: RECORD_TYPE, id: existingId });
    const changes = [];

    // Normal fields: compare by value
    Object.keys(values)
      .filter(f => TEXT_COMPARE_FIELDS.indexOf(f) === -1)
      .forEach(f => {
        const oldV = normForCompare(f, rec.getValue(f));
        const newV = normForCompare(f, values[f]);
        if (oldV !== newV) {
          changes.push({ field: f, from: oldV, to: newV });
          rec.setValue(f, values[f]);
        }
      });

    // List fields: compare by display text (e.g. "2" vs "2")
    TEXT_COMPARE_FIELDS.forEach(f => {
      const oldT = String(rec.getText(f) || '').trim();
      const newT = String(built.texts[f] || '').trim();
      if (newT && oldT !== newT) {
        changes.push({ field: f, from: oldT, to: newT });
        rec.setText(f, newT);
      }
    });

    if (!changes.length) return { action: 'nochange', recordId: existingId, extId };

    if (!DRY_RUN) rec.save();
    return { action: 'update', recordId: existingId, extId, changes };
  }

  // Validates the row and returns { extId, values } - same rules as before
  function buildGrsValues(row) {
    if (!row.custrecord_ft_pool_id) {
      throw new Error('Pool mapping not found (Pool: ' + row.custrecord_ft_pool + ', Vendor: ' + row.custrecord_ft_vendor_number + ')');
    }
    if (!row.custrecord_ft_dc_id) {
      throw new Error('DC not found in Location Master (DC: ' + row.custrecord_ft_dc + ')');
    }

    let mabd = null;
    let adjMabd = null;
    if (row.custrecord_ft_mabd) {
      const p = row.custrecord_ft_mabd.split('-');
      mabd = new Date(p[0], p[1] - 1, p[2]);
      if (isNaN(mabd.getTime())) throw new Error('Invalid MABD date: ' + row.custrecord_ft_mabd);
      adjMabd = getAdjustedFriday(row.custrecord_ft_mabd) || null;
    }

    // Location from Pool mapping; fall back to Walmart Pool mapping record
    const locationId = getPoolLocation(row.mapping.location) || row.mapping.location || '';
    if (!locationId) {
      throw new Error('No location mapping for Pool: ' + row.custrecord_ft_pool);
    }

    const values = {
      custrecord_ft_pool: row.custrecord_ft_pool_id,
      custrecord_ft_dc: row.custrecord_ft_dc_id,
      custrecord_ft_truck: row.custrecord_ft_truck,
      name: row.name,
      custrecord_ft_vendor_number: row.custrecord_ft_vendor_number,
      custrecord_ft_vendor: row.custrecord_ft_vendor,
      custrecord_ft_department: row.custrecord_ft_department,
      custrecord_ft_item_number: row.custrecord_ft_item_number,
      custrecord_ft_item_description: row.custrecord_ft_item_description,
      custrecord_ft_order_quantity: toNum(row.custrecord_ft_order_quantity),
      custrecord_ft_weight_quantity: toNum(row.custrecord_ft_weight_quantity),
      custrecord_ft_cube_quantity: toNum(row.custrecord_ft_cube_quantity),
      custrecord_ft_purchase_order_type: row.custrecord_ft_purchase_order_type,
      custrecord_ft_mabd: mabd,
      custrecord_ft_adj_mabd: adjMabd,
      custrecord_ft_grs_location: locationId,
      custrecord_ft_twovsthree_week_category: row.mapping.weeks || '',
      custrecord_ft_walmart_grs_mapping: row.mapping.internalid || ''
    };

    // External ID: Vendor_Name_Item_AdjMABD(yyyy-mm-dd)
    const extId = row.custrecord_ft_vendor_number + '_' +
      row.name + '_' +
      row.custrecord_ft_item_number + '_' +
      (adjMabd ? ymd(adjMabd) : '');

    // Display text for list fields compared by text on update
    const texts = {
      custrecord_ft_twovsthree_week_category: row.mapping.weeksText || ''
    };

    return { extId, values, texts };
  }

  // Returns internal id of existing record with this external id, or null
  function findByExternalId(extId) {
    let foundId = null;
    search.create({
      type: RECORD_TYPE,
      filters: [['externalid', 'anyof', extId]],
      columns: ['internalid']
    }).run().each(result => {
      foundId = result.id;
      return false;
    });
    return foundId;
  }

  function logUpsert(where, r) {
    const prefix = DRY_RUN ? 'DRY RUN - WOULD ' : '';
    if (r.action === 'create') {
      log.audit(prefix + 'CREATE', where + ' | External ID: ' + r.extId + (r.recordId ? ' | Record ' + r.recordId : ''));
    } else if (r.action === 'update') {
      const diff = r.changes.map(c => c.field + ': "' + c.from + '" -> "' + c.to + '"').join('; ');
      log.audit(prefix + 'UPDATE', where + ' | Record ' + r.recordId + ' | ' + diff);
    } else {
      log.debug((DRY_RUN ? 'DRY RUN - ' : '') + 'NO CHANGE', where + ' | Record ' + r.recordId + ' | External ID: ' + r.extId);
    }
  }

  // ---------------------------------------------------------------------------
  // REDUCE - one call per file: tally results, move file to Processed or Error
  // ---------------------------------------------------------------------------
  function reduce(context) {
    const fileId = context.key;
    let meta = { fileName: '', created: '' };
    let successCount = 0, createdCount = 0, updatedCount = 0, unchangedCount = 0;
    const errors = [];

    context.values.forEach(v => {
      const val = JSON.parse(v);
      if (val.type === 'meta') {
        meta = val;
      } else if (val.status === 'success') {
        successCount++;
        if (val.action === 'create') createdCount++;
        else if (val.action === 'update') updatedCount++;
        else if (val.action === 'nochange') unchangedCount++;
      } else {
        errors.push({ lineNo: val.lineNo, message: val.message, raw: val.raw });
      }
    });

    const hasErrors = errors.length > 0;
    let movedTo;

    // if (DRY_RUN) {
    //   movedTo = 'DRY RUN - not moved (would go to ' + (hasErrors ? 'Error' : 'Processed') + ')';
    // } else {
    //   movedTo = hasErrors ? 'Error' : 'Processed';
    //   try {
    //     const f = file.load({ id: fileId });
    //     f.folder = hasErrors ? ERROR_FOLDER_ID : PROCESSED_FOLDER_ID;
    //     f.save();
    //   } catch (moveErr) {
    //     log.error('File move failed', fileId + ': ' + moveErr.message);
    //     movedTo = 'NOT MOVED (' + moveErr.message + ')';
    //   }
    // }
    if (DRY_RUN) {
      movedTo = 'DRY RUN - not moved (would go to ' + (hasErrors ? 'Error' : 'Processed') + ')';
    } else if (!hasErrors) {
      // Success -> just move to Processed
      movedTo = 'Processed';
      try {
        const f = file.load({ id: fileId });
        f.folder = PROCESSED_FOLDER_ID;
        f.save();
      } catch (moveErr) {
        log.error('File move failed', fileId + ': ' + moveErr.message);
        movedTo = 'NOT MOVED (' + moveErr.message + ')';
      }
    } else {
      // Errors -> write a copy with an "Error" column into the Error folder
      movedTo = 'Error';
      try {
        const original = file.load({ id: fileId });

        // line number -> error message
        const errByLine = {};
        errors.forEach(e => {
          if (e.lineNo) errByLine[e.lineNo] = (errByLine[e.lineNo] ? errByLine[e.lineNo] + ' | ' : '') + e.message;
        });

        const lines = original.getContents().split(/\r?\n/);
        const out = lines.map((line, idx) => {
          if (line.trim() === '') return line;
          const lineNo = idx + 1;
          if (errByLine[lineNo]) return line + ',' + csvCell(errByLine[lineNo]); // failed row
          if (idx === 0 && /Internal ID"?\s*$/.test(line)) return line + ',Error'; // header of update file
          return line + ',';                                                       // good row -> blank
        });

        const errFile = file.create({
          name: original.name,
          fileType: file.Type.CSV,
          contents: out.join('\n'),
          folder: ERROR_FOLDER_ID
        });
        const newId = errFile.save();
        file.delete({ id: fileId }); // remove original from inbound folder
        movedTo = 'Error (file ' + newId + ')';
      } catch (moveErr) {
        // Fallback: couldn't rewrite (e.g. unreadable file) -> just move it as before
        log.error('Error file rewrite failed', fileId + ': ' + moveErr.message);
        try {
          const f = file.load({ id: fileId });
          f.folder = ERROR_FOLDER_ID;
          f.save();
        } catch (e2) {
          movedTo = 'NOT MOVED (' + e2.message + ')';
        }
      }
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
        createdCount,
        updatedCount,
        unchangedCount,
        errorCount: errors.length,
        movedTo,
        errors
      }
    });
  }

  // ---------------------------------------------------------------------------
  // SUMMARIZE - log per-file results; send email (live, or dry run if enabled)
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

    const tag = DRY_RUN ? 'DRY RUN SUMMARY' : 'SUMMARY';
    results.forEach(r => {
      log.audit(tag + ' - ' + r.fileName,
        (DRY_RUN ? 'Would create: ' : 'Created: ') + r.createdCount +
        ' | ' + (DRY_RUN ? 'Would update: ' : 'Updated: ') + r.updatedCount +
        ' | No change: ' + r.unchangedCount +
        ' | Errors: ' + r.errorCount +
        ' | ' + r.movedTo);
    });

   // if (!DRY_RUN || SEND_EMAIL_IN_DRY_RUN) sendResultEmail(results);
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
        '<td style="' + td + ' text-align:center;">' + r.createdCount + '</td>' +
        '<td style="' + td + ' text-align:center;">' + r.updatedCount + '</td>' +
        '<td style="' + td + ' text-align:center;">' + r.unchangedCount + '</td>' +
        '<td style="' + td + ' text-align:center;">' + r.errorCount + '</td>' +
        '<td style="' + td + '">' + esc(r.movedTo) + '</td>' +
        '</tr>';
    }).join('');

    let body = 'Hi Team,<br/><br/>' + (DRY_RUN ? '<b>DRY RUN - no records were changed.</b><br/><br/>' : '') +
      'Walmart GRS file processing results:<br/><br/>' +
      '<table style="border-collapse:collapse; width:100%; font-family:Arial; font-size:12px;">' +
      '<thead><tr style="background:#f2f2f2;">' +
      ['#', 'File Name', 'Date', 'File ID', 'Status', 'Created', 'Updated', 'No Change', 'Errors', 'Moved To']
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
      'Successful rows were created or updated; fix and re-submit only the failed lines.' +
      '<br/><br/>Regards,<br/>Fusion Transport, LLC';

    const subject = (DRY_RUN ? '[DRY RUN] ' : '') + 'Walmart GRS Files Processed - ' +
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

  function ymd(d) {
    return d.getFullYear() + '-' +
      String(d.getMonth() + 1).padStart(2, '0') + '-' +
      String(d.getDate()).padStart(2, '0');
  }

  function toNum(v) {
    const n = parseFloat(v);
    return isNaN(n) ? '' : n;
  }

  // Normalize a value so existing and new values compare correctly
  function normForCompare(fieldId, v) {
    if (v === null || v === undefined || v === '') return '';
    if (DATE_FIELDS.indexOf(fieldId) !== -1) {
      return (v instanceof Date && !isNaN(v.getTime())) ? ymd(v) : String(v);
    }
    if (NUMBER_FIELDS.indexOf(fieldId) !== -1) {
      const n = parseFloat(v);
      return isNaN(n) ? '' : String(n);
    }
    return String(v).trim();
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