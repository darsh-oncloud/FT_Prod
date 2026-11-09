/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 *
 * GRS_CSVProcessing_MR.js
 *
 * Purpose:
 *   Reads Walmart GRS CSV files from a "Pending" File Cabinet folder, resolves each row's
 *   POOL_DC combo to a Sail record (customrecord_ft_walmart_saildates_dcloc) via the
 *   multiselect custrecord_pool_dc_combo_list (display text of each selected value
 *   is a POOL_DC combo, e.g. 9854_6011), then groups rows by
 *   SAIL RECORD NAME + DC + CUSTOMER + ADJUSTED MABD and creates one customrecord_ft_walmart_grs
 *   record per group (comma-separated PO list, summed ORDER_QTY/WEIGHT_QTY/CUBE_QTY,
 *   customer looked up via custentity_ft_vendor_number), then moves the source file to
 *   "Processed".
 *
 *   Sail lookup: one search per chunk of POOL_DC combos, filtering with
 *   formulatext: {custrecord_pool_dc_combo_list} CONTAINS <combo> as a coarse
 *   pre-filter. The multiselect's getText() (comma-separated display text) is then split
 *   into tokens and only EXACT token matches are accepted, so "9854_6011" can't match
 *   inside "19854_6011". Optionally (USE_POOL_DC_TEXT_FALLBACK) the legacy text field
 *   custrecord_ft_sail_pool_and_dc is also checked.
 *
 *   Because several POOL_DC combos can belong to the same Sail record, rows from
 *   different POOLs that share a Sail record (and DC/customer/MABD) land in ONE group.
 *   Rows whose POOL_DC doesn't match any Sail record fall back to grouping by the raw
 *   POOL (key segment NOSAIL_<pool>) so they never merge with unrelated rows.
 *
 *   In addition to the per-customer "Initial Request" record, this script also rolls
 *   those groups up (within the same file) by SAIL NAME + DC + ADJUSTED MABD only - across
 *   whichever customers/vendors share that key - and creates ONE "Consolidated Request"
 *   record for each such rollup. Both record types live on customrecord_ft_walmart_grs and
 *   are distinguished only by custrecord_appointment_record_type
 *   (1 = Initial Request, 2 = Consolidated Request). The consolidated record intentionally
 *   leaves custrecord_ft_vendor and custrecord_ft_vendor_number blank.
 *
 *   Record name / externalid:
 *     Initial      : <SailName>_<DC>_<customerId or vendorNbrs>_<AdjustedMABDserial>
 *     Consolidated : <SailName>_<DC>_CONSOLIDATED_<AdjustedMABDserial>
 *   (SailName is replaced by NOSAIL_<POOL> when no Sail record matched.)
 *
 *   custrecord_ft_quedns_reqid (unchanged): <FusionWarehouseText or POOL>_<DC>_<AdjustedMABDserial>
 *
 *   3-week pool customers: if the customer resolved from VENDOR_NBR has
 *   custentity_ft_3_week_pool checked, its Initial Request record is created exactly as
 *   usual but saved as INACTIVE (isinactive = T). Consolidated records are unaffected.
 *
 *   Excluded customers: if the customer resolved from VENDOR_NBR has
 *   custentity_exclude_from_grs checked, its rows are dropped before grouping - no
 *   Initial record is created and its POs/lines/quantities are not part of any
 *   Consolidated record.
 *
 *   Governance design: map() parses the CSV, runs ONE customer search and ONE (chunked)
 *   Sail search per file, and builds groups. Each group is emitted with its own compound
 *   key so every group gets its OWN reduce() invocation with a fresh governance allowance;
 *   the DC/Pool lookups plus the record create happen there. summarize() builds an error
 *   CSV for any failed groups and moves the original file to Processed or Error.
 *
 *   Customer/DC/Pool/Sail lookups are best-effort: if a match isn't found, that field is
 *   left blank rather than failing the group. A group only lands in the error CSV if the
 *   record.create()/save() call itself throws.
 *
 *   If a file itself cannot be parsed (missing/garbled headers, empty file, etc.) the
 *   ENTIRE original file is moved to the Error folder instead of Processed.
 */
define(['N/file', 'N/log', 'N/record', 'N/search', 'N/format', 'N/runtime', 'N/task'],
    (file, log, record, search, format, runtime, task) => {

        // =========================================================================
        // CONFIG
        // =========================================================================
        // const PENDING_FOLDER_ID = 2486833;  // Pending
        // const PROCESSED_FOLDER_ID = 2486835;  // Processed
        // const ERROR_FOLDER_ID = 2486834;  // Errored

        const PENDING_FOLDER_ID = 2736865;  // Pending
        const PROCESSED_FOLDER_ID = 2736866;  // Processed
        const ERROR_FOLDER_ID = 2736867;  // Errored

        const CUSTOM_RECORD_TYPE = 'customrecord_ft_walmart_grs';
        const GRS_CUSTOMER_FIELD = 'custrecord_ft_customer';
        const CUSTOMER_VENDOR_NBR_FIELD = 'custentity_ft_vendor_number';
        // Customer checkbox: checked = exclude from Initial AND Consolidated GRS.
        const CUSTOMER_NO_GRS_FIELD = 'custentity_exclude_from_grs';

        const CUSTOMER_THREE_WEEK_POOL_FIELD = 'custentity_ft_3_week_pool';

        const LOCATION_MASTER_RECORD_TYPE = 'customrecord_ft_location_master';
        const LOCATION_MASTER_DC_NBR_FIELD = 'custrecord_ft_wmdc_number';

        const POOL_MAPPING_RECORD_TYPE = 'customrecord_ft_walmart_grs_mapping';
        const POOL_MAPPING_POOL_FIELD = 'custrecord_ft_wmgrsmap_pool';

        const SAIL_RECORD_TYPE = 'customrecord_ft_walmart_saildates_dcloc';
        // Multiselect - each selected value's display text is a POOL_DC combo (e.g. 9854_6011).
        const SAIL_RECORD_POOL_DC_LIST_FIELD = 'custrecord_pool_and_dc_combos';
        // Legacy comma-separated text field. Only used when USE_POOL_DC_TEXT_FALLBACK = true.
        const SAIL_RECORD_POOL_DC_TEXT_FIELD = 'custrecord_ft_sail_pool_and_dc';
        const USE_POOL_DC_TEXT_FALLBACK = false;
        const SAIL_RECORD_POOL_FIELD = 'custrecord_ft_walmart_pool';
        const SAIL_RECORD_FUSION_WAREHOUSE_FIELD = 'custrecord_ft_wmsail_fusion_warehouse';
        const SAIL_RECORD_MABD_DOW_FIELD = 'custrecord_ft_wmsail_mabd';

        // Max POOL_DC values per Sail search (formula filters are heavier, so keep this modest).
        const SAIL_SEARCH_CHUNK_SIZE = 25;

        const APPOINTMENT_RECORD_TYPE_FIELD = 'custrecord_appointment_record_type';
        const RECORD_TYPE_INITIAL = 1;      // "Initial Request"
        const RECORD_TYPE_CONSOLIDATED = 2; // "Consolidated Request"

        // Per-execution caches (reset whenever a new processor picks up work).
        const dcLookupCache = {};         // DC number -> location master id
        const poolLookupCache = {};       // POOL number -> grs mapping id (fallback only)
        const vendorCustomerCache = {};   // VENDOR_NBR -> customer id (or null)
        const customerThreeWeekCache = {};// customer id -> true/false (custentity_ft_3_week_pool)
        const customerNoGrsCache = {};    // customer id -> true/false (custentity_exclude_from_grs)
        const sailByComboCache = {};      // "POOL_DC" -> sail object (or null)

        const REQUIRED_HEADERS = [
            'POOL', 'DC', 'TRUCK', 'PO', 'VENDOR_NBR', 'VENDOR_NAME',
            'DEPT', 'ITEM_NBR', 'ITEM_DESC', 'ORDER_QTY', 'WEIGHT_QTY',
            'CUBE_QTY', 'MABD', 'PO_TYPE'
        ];

        // =========================================================================
        // getInputData - find all CSV files sitting in the Pending folder
        // =========================================================================
        const getInputData = (inputContext) => {
            try {
                const fileSearch = search.create({
                    type: 'file',
                    filters: [
                        ['folder', 'anyof', PENDING_FOLDER_ID],
                        'AND',
                        ['filetype', 'anyof', 'CSV']
                    ],
                    columns: ['name']
                });

                const fileIds = [];
                fileSearch.run().each((result) => {
                    fileIds.push(result.id);
                    return true;
                });

                log.audit('getInputData', `Found ${fileIds.length} CSV file(s) in Pending folder`);
                return fileIds;
            } catch (e) {
                log.error('getInputData error', e);
                throw e;
            }
        };

        // =========================================================================
        // map - one invocation per file: parse, resolve customers + sail records
        // (one search each per file), group, and emit one key per group.
        // =========================================================================
        const map = (mapContext) => {
            const fileId = mapContext.value;
            let grsFile;

            try {
                grsFile = file.load({ id: fileId });
            } catch (e) {
                log.error(`map - unable to load file ${fileId}`, e);
                mapContext.write({ key: `${fileId}::__FATAL__`, value: JSON.stringify({ fileId, status: 'fatal', error: e.message }) });
                return;
            }

            let rows;
            try {
                rows = parseCsv(grsFile.getContents());
                validateHeaders(rows.headerRow);
            } catch (e) {
                log.error(`map - unable to parse file ${grsFile.name} (${fileId})`, e);
                mapContext.write({ key: `${fileId}::__FATAL__`, value: JSON.stringify({ fileId, status: 'fatal', error: e.message }) });
                return;
            }

            const vendorNbrs = uniqueVendorNbrs(rows.dataRows, rows.headerIndex);
            const { vendorCustomerMap, threeWeekByCustomer, noGrsByCustomer } = lookupCustomerIdsForVendorNbrs(vendorNbrs);
            const eligibleRows = rows.dataRows.filter((row) => {
                const vendorNbr = (row[rows.headerIndex.VENDOR_NBR] || '').trim();
                const customerId = vendorCustomerMap[vendorNbr];
                return !customerId || !noGrsByCustomer[customerId];
            });
            const skippedRows = rows.dataRows.length - eligibleRows.length;
            if (skippedRows > 0) {
                log.audit('map - NO GRS customers excluded',
                    `file ${fileId}: skipped ${skippedRows} row(s) before initial and consolidated grouping`);
            }
            if (eligibleRows.length === 0) {
                // Ensure summarize still moves an entirely excluded file to Processed.
                mapContext.write({ key: `${fileId}::__SKIPPED__`, value: JSON.stringify({
                    fileId, status: 'skipped', skippedRows
                }) });
                return;
            }

            const poolDcCombos = uniquePoolDcCombos(eligibleRows, rows.headerIndex);
            const sailByCombo = lookupSailRecordsForPoolDcCombos(poolDcCombos);

            const groups = groupRows(eligibleRows, rows.headerIndex, vendorCustomerMap, threeWeekByCustomer, sailByCombo);

            Object.keys(groups).forEach((groupKey) => {
                mapContext.write({
                    key: `${fileId}::${groupKey}`,
                    value: JSON.stringify({
                        fileId,
                        groupKey,
                        type: 'initial',
                        headerRow: rows.headerRow,
                        group: groups[groupKey]
                    })
                });
            });

            const consolidatedGroups = buildConsolidatedGroups(groups);

            Object.keys(consolidatedGroups).forEach((rollupKey) => {
                mapContext.write({
                    key: `${fileId}::CONSOLIDATED::${rollupKey}`,
                    value: JSON.stringify({
                        fileId,
                        groupKey: rollupKey,
                        type: 'consolidated',
                        headerRow: rows.headerRow,
                        group: consolidatedGroups[rollupKey]
                    })
                });
            });
        };

        // =========================================================================
        // reduce - one invocation per GROUP
        // =========================================================================
        const reduce = (reduceContext) => {
            const entry = JSON.parse(reduceContext.values[0]);

            if (entry.status === 'fatal' || entry.status === 'skipped') {
                reduceContext.write({ key: entry.fileId, value: JSON.stringify(entry) });
                return;
            }

            const { fileId, groupKey, headerRow, group, type } = entry;
            const isConsolidated = type === 'consolidated';
            const recordTypeValue = isConsolidated ? RECORD_TYPE_CONSOLIDATED : RECORD_TYPE_INITIAL;

            try {
                if (!isConsolidated && !group.customerId) {
                    log.audit(`reduce - no customer found for VENDOR_NBR(s) ${group.vendorNbrs.join(', ')}`, `group ${groupKey} - custrecord_ft_vendor left blank`);
                }

                const recId = createGrsRecord(group, recordTypeValue, isConsolidated);

                reduceContext.write({
                    key: fileId,
                    value: JSON.stringify({ status: 'success', groupKey, type, recordId: recId })
                });
            } catch (e) {
                log.error(`reduce - group failed [${groupKey}] in file ${fileId}`, e);
                reduceContext.write({
                    key: fileId,
                    value: JSON.stringify({
                        status: 'error',
                        groupKey,
                        type,
                        error: e.message,
                        headerRow,
                        rows: group.rawRows
                    })
                });
            }
        };

        // =========================================================================
        // summarize
        // =========================================================================
        const summarize = (summaryContext) => {
            const byFile = {};
            let totalSuccesses = 0;

            summaryContext.output.iterator().each((fileId, value) => {
                const entry = JSON.parse(value);
                if (!byFile[fileId]) byFile[fileId] = [];
                byFile[fileId].push(entry);
                return true;
            });

            Object.keys(byFile).forEach((fileId) => {
                const entries = byFile[fileId];

                let grsFile;
                try {
                    grsFile = file.load({ id: fileId });
                } catch (e) {
                    log.error(`summarize - unable to load file ${fileId} for final move`, e);
                    return;
                }

                const fatal = entries.filter((e) => e.status === 'fatal');
                if (fatal.length > 0) {
                    moveFile(grsFile, ERROR_FOLDER_ID);
                    log.error(`summarize - file ${grsFile.name} unparseable, moved to Error folder`, fatal[0].error);
                    return;
                }

                const errors = entries.filter((e) => e.status === 'error');
                const successes = entries.filter((e) => e.status === 'success');
                totalSuccesses += successes.length;

                if (errors.length > 0) {
                    writeErrorCsv(grsFile, errors);
                }

                moveFile(grsFile, PROCESSED_FOLDER_ID);

                const initialSuccesses = successes.filter((e) => e.type !== 'consolidated').length;
                const consolidatedSuccesses = successes.filter((e) => e.type === 'consolidated').length;

                log.audit(`summarize - file ${grsFile.name} complete`,
                    `${initialSuccesses} initial record(s), ${consolidatedSuccesses} consolidated record(s) created, ${errors.length} group(s) failed`);
            });

            let mapErrors = 0;
            summaryContext.mapSummary.errors.iterator().each((key, error) => {
                mapErrors++;
                log.error(`Map error on key ${key}`, error);
                return true;
            });

            let reduceErrors = 0;
            summaryContext.reduceSummary.errors.iterator().each((key, error) => {
                reduceErrors++;
                log.error(`Reduce error on key ${key}`, error);
                return true;
            });

            log.audit('summarize', `Usage: ${summaryContext.usage} | Concurrency: ${summaryContext.concurrency} `
                + `| Yields: ${summaryContext.yields} | Map errors: ${mapErrors} | Reduce errors: ${reduceErrors}`);

            if (totalSuccesses > 0) {
                try {
                    const scriptObj = runtime.getCurrentScript();
                    const apptScriptId = scriptObj.getParameter({ name: 'custscript_qued_appt_script_id' }) || 'customscript_ft_qued_appointment_mr';
                    const apptDeployId = scriptObj.getParameter({ name: 'custscript_qued_appt_deploy_id' }) || 'customdeploy_ft_qued_appointment_mr';

                    log.audit('summarize', `Scheduling Qued Appointment Map/Reduce Script: ${apptScriptId} (Deployment: ${apptDeployId || 'Default'})`);

                    const taskParams = {
                        taskType: task.TaskType.MAP_REDUCE,
                        scriptId: apptScriptId
                    };
                    if (apptDeployId) {
                        taskParams.deploymentId = apptDeployId;
                    }

                    const mrTask = task.create(taskParams);
                    const mrTaskId = mrTask.submit();
                    log.audit('summarize', `Qued Appointment Map/Reduce Task submitted successfully. Task ID: ${mrTaskId}`);
                } catch (taskError) {
                    log.error('summarize - failed to schedule appointment MR', taskError.message || taskError.toString());
                }
            } else {
                log.audit('summarize', 'No GRS records were created; skipping Qued Appointment Map/Reduce script scheduling.');
            }
        };


        // =========================================================================
        // Helpers - CSV
        // =========================================================================

        function parseCsv(contents) {
            const lines = contents
                .replace(/\r\n/g, '\n')
                .replace(/\r/g, '\n')
                .split('\n')
                .filter((line) => line.trim().length > 0);

            if (lines.length < 2) {
                throw new Error('File has no data rows (needs a header row plus at least one data row)');
            }

            const headerRow = parseCsvLine(lines[0]).map((h) => h.trim().replace(/^\uFEFF/, ''));
            const headerIndex = {};
            headerRow.forEach((h, i) => { headerIndex[h] = i; });

            const dataRows = lines.slice(1).map((line) => parseCsvLine(line));

            return { headerRow, headerIndex, dataRows };
        }

        function parseCsvLine(line) {
            const fields = [];
            let cur = '';
            let inQuotes = false;

            for (let i = 0; i < line.length; i++) {
                const ch = line[i];
                if (inQuotes) {
                    if (ch === '"') {
                        if (line[i + 1] === '"') { cur += '"'; i++; }
                        else { inQuotes = false; }
                    } else {
                        cur += ch;
                    }
                } else {
                    if (ch === '"') { inQuotes = true; }
                    else if (ch === ',') { fields.push(cur); cur = ''; }
                    else { cur += ch; }
                }
            }
            fields.push(cur);
            return fields;
        }

        function validateHeaders(headerRow) {
            const missing = REQUIRED_HEADERS.filter((h) => headerRow.indexOf(h) === -1);
            if (missing.length > 0) {
                throw new Error(`Missing required column(s): ${missing.join(', ')}`);
            }
        }

        // =========================================================================
        // Helpers - grouping
        // =========================================================================

        /**
         * Groups data rows by SAIL NAME + DC + (resolved customer) + ADJUSTED MABD.
         *
         * - Sail name comes from the Sail record matched on the row's POOL_DC combo.
         *   No match -> NOSAIL_<pool>, so unmatched pools don't merge with each other.
         * - Customer comes from vendorCustomerMap. No match -> UNMATCHED_<vendorNbr>.
         * - isThreeWeekPool is taken from the resolved customer's custentity_ft_3_week_pool.
         *   Customer is part of the key, so the flag is uniform within a group.
         *
         * A group can span multiple POOLs (all belonging to the same Sail record),
         * so pools are collected into an array.
         */
        function groupRows(dataRows, headerIndex, vendorCustomerMap, threeWeekByCustomer, sailByCombo) {
            const groups = {};

            dataRows.forEach((row) => {
                const pool = (row[headerIndex.POOL] || '').trim();
                const dc = (row[headerIndex.DC] || '').trim();
                const truck = (row[headerIndex.TRUCK] || '').trim();
                const vendorNbr = (row[headerIndex.VENDOR_NBR] || '').trim();
                const mabd = (row[headerIndex.MABD] || '').trim();
                const po = (row[headerIndex.PO] || '').trim();

                const orderQty = parseFloat(row[headerIndex.ORDER_QTY]) || 0;
                const weightQty = parseFloat(row[headerIndex.WEIGHT_QTY]) || 0;
                const cubeQty = parseFloat(row[headerIndex.CUBE_QTY]) || 0;

                const sail = sailByCombo[`${pool}_${dc}`] || null;
                const sailKey = sail ? sail.name : `NOSAIL_${pool}`;

                const customerId = vendorCustomerMap[vendorNbr] || null;
                const customerKey = customerId || `UNMATCHED_${vendorNbr}`;

                const adjustedMabdSerial = adjustedSerialForMabd(mabd, sail);
                const groupKey = `${sailKey}_${dc}_${customerKey}_${adjustedMabdSerial}`;

                if (!groups[groupKey]) {
                    groups[groupKey] = {
                        sailKey, sail, dc, mabd, adjustedMabdSerial,
                        customerId,
                        isThreeWeekPool: !!(customerId && threeWeekByCustomer[customerId]),
                        pools: [],
                        vendorNbrs: [],
                        poNumbers: [],
                        truckNumbers: [],
                        orderQty: 0,
                        weightQty: 0,
                        cubeQty: 0,
                        poTotals: {},
                        rawRows: []
                    };
                }

                const g = groups[groupKey];
                if (pool && g.pools.indexOf(pool) === -1) g.pools.push(pool);
                if (vendorNbr && g.vendorNbrs.indexOf(vendorNbr) === -1) g.vendorNbrs.push(vendorNbr);
                if (po && g.poNumbers.indexOf(po) === -1) g.poNumbers.push(po);
                if (po) {
                    if (!g.poTotals[po]) g.poTotals[po] = { po: po, orderQty: 0, weightQty: 0, cubeQty: 0 };
                    g.poTotals[po].orderQty += orderQty;
                    g.poTotals[po].weightQty += weightQty;
                    g.poTotals[po].cubeQty += cubeQty;
                }
                if (truck && g.truckNumbers.indexOf(truck) === -1) g.truckNumbers.push(truck);
                g.orderQty += orderQty;
                g.weightQty += weightQty;
                g.cubeQty += cubeQty;
                g.rawRows.push(row);
            });

            return groups;
        }

        /**
         * Rolls the per-customer groups up by SAIL NAME + DC + ADJUSTED MABD, across customers,
         * within the same file. Pools/POs/trucks are unioned, quantities summed.
         */
        function buildConsolidatedGroups(groups) {
            const consolidated = {};

            Object.keys(groups).forEach((key) => {
                const g = groups[key];
                const rollupKey = `${g.sailKey}_${g.dc}_${g.adjustedMabdSerial}`;

                if (!consolidated[rollupKey]) {
                    consolidated[rollupKey] = {
                        sailKey: g.sailKey, sail: g.sail, dc: g.dc, mabd: g.mabd,
                        adjustedMabdSerial: g.adjustedMabdSerial,
                        pools: [],
                        poNumbers: [],
                        truckNumbers: [],
                        orderQty: 0,
                        weightQty: 0,
                        cubeQty: 0,
                        poTotals: {},
                        rawRows: []
                    };
                }

                const c = consolidated[rollupKey];
                g.pools.forEach((p) => { if (c.pools.indexOf(p) === -1) c.pools.push(p); });
                g.poNumbers.forEach((po) => { if (c.poNumbers.indexOf(po) === -1) c.poNumbers.push(po); });
                g.truckNumbers.forEach((t) => { if (c.truckNumbers.indexOf(t) === -1) c.truckNumbers.push(t); });

                Object.keys(g.poTotals).forEach((po) => {
                    if (!c.poTotals[po]) c.poTotals[po] = { po: po, orderQty: 0, weightQty: 0, cubeQty: 0 };
                    c.poTotals[po].orderQty += g.poTotals[po].orderQty;
                    c.poTotals[po].weightQty += g.poTotals[po].weightQty;
                    c.poTotals[po].cubeQty += g.poTotals[po].cubeQty;
                });

                c.orderQty += g.orderQty;
                c.weightQty += g.weightQty;
                c.cubeQty += g.cubeQty;
                c.rawRows.push(...g.rawRows);
            });

            return consolidated;
        }

        function uniqueVendorNbrs(dataRows, headerIndex) {
            const seen = {};
            const result = [];
            dataRows.forEach((row) => {
                const v = (row[headerIndex.VENDOR_NBR] || '').trim();
                if (v && !seen[v]) {
                    seen[v] = true;
                    result.push(v);
                }
            });
            return result;
        }

        /** Returns the distinct, non-blank "POOL_DC" combos present in a file. */
        function uniquePoolDcCombos(dataRows, headerIndex) {
            const seen = {};
            const result = [];
            dataRows.forEach((row) => {
                const pool = (row[headerIndex.POOL] || '').trim();
                const dc = (row[headerIndex.DC] || '').trim();
                if (!pool || !dc) return;
                const combo = `${pool}_${dc}`;
                if (!seen[combo]) {
                    seen[combo] = true;
                    result.push(combo);
                }
            });
            return result;
        }

        function buildOrFilterChain(fieldId, values, operator) {
            const op = operator || 'is';
            const expr = [];
            values.forEach((v, i) => {
                if (i > 0) expr.push('OR');
                expr.push([fieldId, op, v]);
            });
            return expr;
        }

        /**
         * Sail search filter for a chunk of POOL_DC combos:
         *   formulatext: {multiselect} CONTAINS combo
         *   (OR legacy text field CONTAINS combo, when USE_POOL_DC_TEXT_FALLBACK is on)
         * {multiselect} in a formula resolves to the selected values' display text,
         * comma-separated. 'contains' is only a coarse pre-filter - exact token matching
         * happens on the results.
         */
        function buildSailComboFilter(combos) {
            const expr = [];
            combos.forEach((combo, i) => {
                if (i > 0) expr.push('OR');
                expr.push([`formulatext: {${SAIL_RECORD_POOL_DC_LIST_FIELD}}`, 'contains', combo]);
                if (USE_POOL_DC_TEXT_FALLBACK) {
                    expr.push('OR');
                    expr.push([SAIL_RECORD_POOL_DC_TEXT_FIELD, 'contains', combo]);
                }
            });
            return expr;
        }

        function chunkArray(arr, size) {
            const out = [];
            for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
            return out;
        }

        // =========================================================================
        // Helpers - lookups
        // =========================================================================

        function lookupCustomerIdsForVendorNbrs(vendorNbrs) {
            const uncached = vendorNbrs.filter(
                (v) => !Object.prototype.hasOwnProperty.call(vendorCustomerCache, v)
            );

            if (uncached.length > 0) {
                const uncachedSet = {};
                uncached.forEach((v) => { uncachedSet[v] = true; });

                const noGrsField = CUSTOMER_NO_GRS_FIELD.trim();
                const columns = ['internalid', CUSTOMER_VENDOR_NBR_FIELD, CUSTOMER_THREE_WEEK_POOL_FIELD];
                if (noGrsField) columns.push(noGrsField);
                const custSearch = search.create({
                    type: search.Type.CUSTOMER,
                    filters: [
                        buildOrFilterChain(CUSTOMER_VENDOR_NBR_FIELD, uncached, 'contains'),
                        'AND',
                        ['isinactive', 'is', 'F']
                    ],
                    columns
                });

                custSearch.run().each((result) => {
                    const rawFieldValue = result.getValue({ name: CUSTOMER_VENDOR_NBR_FIELD }) || '';
                    const tokens = rawFieldValue.split(';').map((t) => t.trim()).filter(Boolean);
                    const threeWeekValue = result.getValue({ name: CUSTOMER_THREE_WEEK_POOL_FIELD });
                    let matched = false;
                    tokens.forEach((token) => {
                        if (uncachedSet[token]) {
                            vendorCustomerCache[token] = result.id;
                            matched = true;
                        }
                    });
                    if (matched) {
                        customerThreeWeekCache[result.id] = threeWeekValue === true || threeWeekValue === 'T';
                        const noGrsValue = noGrsField ? result.getValue({ name: noGrsField }) : false;
                        customerNoGrsCache[result.id] = noGrsValue === true || noGrsValue === 'T';
                    }
                    return true;
                });

                uncached.forEach((v) => {
                    if (!Object.prototype.hasOwnProperty.call(vendorCustomerCache, v)) {
                        vendorCustomerCache[v] = null;
                    }
                });
            }

            const vendorCustomerMap = {};
            const threeWeekByCustomer = {};
            const noGrsByCustomer = {};
            vendorNbrs.forEach((v) => {
                const custId = vendorCustomerCache[v];
                vendorCustomerMap[v] = custId;
                if (custId) {
                    threeWeekByCustomer[custId] = !!customerThreeWeekCache[custId];
                    noGrsByCustomer[custId] = !!customerNoGrsCache[custId];
                }
            });
            return { vendorCustomerMap, threeWeekByCustomer, noGrsByCustomer };
        }

        /**
         * Splits a POOL_DC list value into individual POOL_DC tokens.
         *
         * Handles multiselect getText() output ("9854_6011,11026_6011" or with spaces after
         * commas) and the legacy text field. Defensive: a token with more than two parts and
         * an even part count (e.g. "11002_6011_11026_6011", where a comma was missed) is
         * split into consecutive POOL_DC pairs.
         */
        function parsePoolDcTokens(rawValue) {
            const tokens = [];
            String(rawValue || '')
                .split(/[,;\s]+/)
                .map((t) => t.trim())
                .filter(Boolean)
                .forEach((token) => {
                    const parts = token.split('_').filter(Boolean);
                    if (parts.length === 2) {
                        tokens.push(`${parts[0]}_${parts[1]}`);
                    } else if (parts.length > 2 && parts.length % 2 === 0) {
                        for (let i = 0; i < parts.length; i += 2) {
                            tokens.push(`${parts[i]}_${parts[i + 1]}`);
                        }
                    }
                });
            return tokens;
        }

        /**
         * Resolves "POOL_DC" combos to Sail records via the multiselect
         * custrecord_pool_dc_combo_list (display text of each selected value).
         *
         * One search per chunk of combos, using formulatext CONTAINS as a coarse pre-filter.
         * The multiselect's getText() is then tokenised and only EXACT token matches are
         * accepted. Cached across map() invocations. If a combo appears on more than one
         * active Sail record, the first one found is kept and a warning is logged.
         *
         * Returns a map of combo -> { id, name, poolId, fusionWarehouse,
         * fusionWarehouseText, mabdDow } (or null if no match).
         */
        function lookupSailRecordsForPoolDcCombos(combos) {
            const uncached = combos.filter(
                (c) => !Object.prototype.hasOwnProperty.call(sailByComboCache, c)
            );

            if (uncached.length > 0) {
                const uncachedSet = {};
                uncached.forEach((c) => { uncachedSet[c] = true; });

                const columns = [
                    'internalid',
                    'name',
                    SAIL_RECORD_POOL_DC_LIST_FIELD,
                    SAIL_RECORD_POOL_FIELD,
                    SAIL_RECORD_FUSION_WAREHOUSE_FIELD,
                    SAIL_RECORD_MABD_DOW_FIELD
                ];
                if (USE_POOL_DC_TEXT_FALLBACK) columns.push(SAIL_RECORD_POOL_DC_TEXT_FIELD);

                chunkArray(uncached, SAIL_SEARCH_CHUNK_SIZE).forEach((chunk) => {
                    const sailSearch = search.create({
                        type: SAIL_RECORD_TYPE,
                        filters: [
                            buildSailComboFilter(chunk),
                            'AND',
                            ['isinactive', 'is', 'F']
                        ],
                        columns
                    });

                    sailSearch.run().each((result) => {
                        // Multiselect getText() -> selected values' display text, comma-separated.
                        const listTokens = parsePoolDcTokens(result.getText({ name: SAIL_RECORD_POOL_DC_LIST_FIELD }));
                        const textTokens = USE_POOL_DC_TEXT_FALLBACK
                            ? parsePoolDcTokens(result.getValue({ name: SAIL_RECORD_POOL_DC_TEXT_FIELD }))
                            : [];
                        const tokens = listTokens.concat(textTokens.filter((t) => listTokens.indexOf(t) === -1));

                        const fusionValue = result.getValue({ name: SAIL_RECORD_FUSION_WAREHOUSE_FIELD });

                        const sail = {
                            id: result.id,
                            name: String(result.getValue({ name: 'name' }) || '').trim(),
                            poolId: result.getValue({ name: SAIL_RECORD_POOL_FIELD }) || null,
                            fusionWarehouse: fusionValue,
                            fusionWarehouseText: result.getText({ name: SAIL_RECORD_FUSION_WAREHOUSE_FIELD }) || fusionValue,
                            mabdDow: sailMabdValueToJsDow(result.getValue({ name: SAIL_RECORD_MABD_DOW_FIELD }))
                        };

                        tokens.forEach((token) => {
                            if (!uncachedSet[token]) return;
                            const existing = sailByComboCache[token];
                            if (existing && existing.id !== sail.id) {
                                log.audit(`lookupSailRecords - POOL_DC ${token} on multiple Sail records`,
                                    `keeping ${existing.id} (${existing.name}), ignoring ${sail.id} (${sail.name})`);
                                return;
                            }
                            sailByComboCache[token] = sail;
                        });
                        return true;
                    });
                });

                uncached.forEach((c) => {
                    if (!Object.prototype.hasOwnProperty.call(sailByComboCache, c)) {
                        sailByComboCache[c] = null;
                        log.audit('lookupSailRecords - no Sail record', `POOL_DC ${c} - grouping falls back to POOL`);
                    }
                });
            }

            const result = {};
            combos.forEach((c) => { result[c] = sailByComboCache[c]; });
            return result;
        }

        function lookupPoolMappingId(poolNumber) {
            if (!poolNumber) return null;
            if (Object.prototype.hasOwnProperty.call(poolLookupCache, poolNumber)) {
                return poolLookupCache[poolNumber];
            }

            let foundId = null;
            const poolSearch = search.create({
                type: POOL_MAPPING_RECORD_TYPE,
                filters: [
                    [POOL_MAPPING_POOL_FIELD, 'is', poolNumber],
                    'AND',
                    ['isinactive', 'is', 'F']
                ],
                columns: ['internalid']
            });
            poolSearch.run().each((result) => {
                foundId = result.id;
                return false;
            });

            poolLookupCache[poolNumber] = foundId;
            return foundId;
        }

        function lookupLocationMasterId(dcNumber) {
            if (Object.prototype.hasOwnProperty.call(dcLookupCache, dcNumber)) {
                return dcLookupCache[dcNumber];
            }

            let foundId = null;
            const locSearch = search.create({
                type: LOCATION_MASTER_RECORD_TYPE,
                filters: [
                    [LOCATION_MASTER_DC_NBR_FIELD, 'is', dcNumber],
                    'AND',
                    ['isinactive', 'is', 'F']
                ],
                columns: ['internalid']
            });
            locSearch.run().each((result) => {
                foundId = result.id;
                return false;
            });

            dcLookupCache[dcNumber] = foundId;
            return foundId;
        }

        // =========================================================================
        // Helpers - dates
        // =========================================================================

        // Use the same adjusted date for grouping, identifiers, and the saved date.
        // When original dates merge, custrecord_ft_mabd retains the first source date.
        function adjustedDateForMabd(mabd, sail) {
            const targetDow = sail && sail.mabdDow !== null && sail.mabdDow !== undefined
                ? sail.mabdDow : 5;
            return getAdjustedMabdDate(mabd, targetDow);
        }

        function excelSerialFromDate(date) {
            if (!date || isNaN(date.getTime())) return '';
            return Math.round((Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())
                - Date.UTC(1899, 11, 30)) / 86400000);
        }

        function adjustedSerialForMabd(mabd, sail) {
            return excelSerialFromDate(adjustedDateForMabd(mabd, sail));
        }

        function normalizeTwoDigitYear(year) {
            return year < 100 ? year + 2000 : year;
        }

        /**
         * Returns the target weekday for the same "week cycle" as the given M/D/YY date:
         * offset 1-4 days after target -> prior occurrence; 5-6 -> next occurrence.
         */
        function getAdjustedMabdDate(dateStr, targetDow) {
            if (!dateStr) return null;
            const parts = String(dateStr).trim().split('/');
            if (parts.length !== 3) return null;

            const month = parseInt(parts[0], 10);
            const day = parseInt(parts[1], 10);
            const year = normalizeTwoDigitYear(parseInt(parts[2], 10));
            if (!month || !day || !year) return null;

            const d = new Date(year, month - 1, day);
            const dow = d.getDay();

            if (dow === targetDow) return d;

            const offset = (dow - targetDow + 7) % 7;

            if (offset >= 1 && offset <= 4) {
                d.setDate(d.getDate() - offset);
            } else {
                d.setDate(d.getDate() + (7 - offset));
            }
            return d;
        }

        /** Sail custrecord_ft_wmsail_mabd (1=Sun...7=Sat) -> JS getDay() (0=Sun...6=Sat). */
        function sailMabdValueToJsDow(sailMabdValue) {
            const n = parseInt(sailMabdValue, 10);
            if (!n || n < 1 || n > 7) return null;
            return n - 1;
        }

        /** M/D/YY(YY) -> Excel serial (e.g. 7/10/2026 -> 46213). '' if unparseable. */
        function excelSerialFromDateString(dateStr) {
            if (!dateStr) return '';
            const parts = String(dateStr).trim().split('/');
            if (parts.length !== 3) return '';

            const month = parseInt(parts[0], 10);
            const day = parseInt(parts[1], 10);
            const year = normalizeTwoDigitYear(parseInt(parts[2], 10));
            if (!month || !day || !year) return '';

            const dateUtc = Date.UTC(year, month - 1, day);
            const excelEpochUtc = Date.UTC(1899, 11, 30);
            return Math.round((dateUtc - excelEpochUtc) / 86400000);
        }

        // =========================================================================
        // Record create
        // =========================================================================

        /**
         * Creates the customrecord_ft_walmart_grs record for a group.
         *
         * name / externalid:
         *   Initial      : <SailName>_<DC>_<customerId or vendorNbrs>_<AdjustedMABDserial>
         *   Consolidated : <SailName>_<DC>_CONSOLIDATED_<AdjustedMABDserial>
         *   (SailName = NOSAIL_<POOL> when no Sail record matched)
         *
         * custrecord_ft_quedns_reqid: <FusionWarehouseText or POOL>_<DC>_<AdjustedMABDserial>
         *   Same value for an initial record and its consolidated record.
         *
         * custrecord_ft_pool: taken from the Sail record's custrecord_ft_walmart_pool when
         * matched (a group can span several CSV POOLs); otherwise looked up from the
         * group's CSV POOL.
         *
         * Initial records for a customer with custentity_ft_3_week_pool checked are saved
         * INACTIVE; everything else about them is identical.
         *
         * Skips (returns existing id) if a record with the same externalid already exists.
         */
        function createGrsRecord(group, recordTypeValue, isConsolidated) {
    const sailRecord = group.sail || null;
    const adjMabd = adjustedDateForMabd(group.mabd, sailRecord);
    const mabdSerial = excelSerialFromDate(adjMabd);
    const firstPool = group.pools[0] || '';

    // Customer found by VENDOR_NBR lookup (same id used in the name)
    const customerId = (!isConsolidated && group.customerId) ? Number(group.customerId) : null;

    const externalId = isConsolidated
        ? `${group.sailKey}_${group.dc}_CONSOLIDATED_${mabdSerial}`
        : `${group.sailKey}_${group.dc}_${group.customerId || group.vendorNbrs.join('-')}_${mabdSerial}`;

    // ---- Existing record: don't recreate, but make sure the customer is set ----
    const existing = findExistingByExternalId(externalId);
    if (existing) {
        if (customerId) {
            setCustomerOnExisting(existing, customerId, externalId);
        }
        log.audit('createGrsRecord - skipped (already exists)', `externalid ${externalId} -> record ${existing}`);
        return existing;
    }

    const locationMasterId = lookupLocationMasterId(group.dc);
    if (!locationMasterId) {
        log.audit(`createGrsRecord - no ${LOCATION_MASTER_RECORD_TYPE} for DC ${group.dc}`, `externalid ${externalId} - custrecord_ft_dc left blank`);
    }

    const poolMappingId = (sailRecord && sailRecord.poolId) || lookupPoolMappingId(firstPool);
    if (!poolMappingId) {
        log.audit(`createGrsRecord - no pool mapping for POOL ${group.pools.join(',')}`, `externalid ${externalId} - custrecord_ft_pool left blank`);
    }

    if (!sailRecord) {
        log.audit(`createGrsRecord - no ${SAIL_RECORD_TYPE} match`,
            `externalid ${externalId} - custrecord_sail_record / custrecord_ft_grs_location left blank`);
    }

    const locationText = String((sailRecord && sailRecord.fusionWarehouseText) || '').trim();
    const quednsReqId = `${locationText || firstPool}_${group.dc}_${mabdSerial}`;

    const rec = record.create({ type: CUSTOM_RECORD_TYPE, isDynamic: false });

    rec.setValue({ fieldId: 'externalid', value: externalId });
    rec.setValue({ fieldId: 'name', value: externalId });
    rec.setValue({ fieldId: APPOINTMENT_RECORD_TYPE_FIELD, value: recordTypeValue });
    if (!isConsolidated && group.isThreeWeekPool) {
        rec.setValue({ fieldId: 'isinactive', value: true });
        log.audit('createGrsRecord - 3-week pool customer, creating INACTIVE',
            `externalid ${externalId} - customer ${group.customerId}`);
    }
    rec.setValue({ fieldId: 'custrecord_ft_quedns_reqid', value: quednsReqId });
    if (poolMappingId) rec.setValue({ fieldId: 'custrecord_ft_pool', value: poolMappingId });
    if (locationMasterId) rec.setValue({ fieldId: 'custrecord_ft_dc', value: locationMasterId });
    if (sailRecord) {
        rec.setValue({ fieldId: 'custrecord_sail_record', value: sailRecord.id });
        if (sailRecord.fusionWarehouse) {
            rec.setValue({ fieldId: 'custrecord_ft_grs_location', value: sailRecord.fusionWarehouse });
        }
    }

    if (!isConsolidated) {
        if (customerId) {
            rec.setValue({ fieldId: 'custrecord_ft_vendor', value: customerId });
            rec.setValue({ fieldId: GRS_CUSTOMER_FIELD, value: customerId });   // <-- NEW
        } else {
            log.audit('createGrsRecord - no customer for VENDOR_NBR(s)',
                `${group.vendorNbrs.join(',')} - ${GRS_CUSTOMER_FIELD} left blank`);
        }
        if (group.vendorNbrs.length > 0) {
            rec.setValue({ fieldId: 'custrecord_ft_vendor_number', value: group.vendorNbrs.join(',') });
        }
    }

    if (group.truckNumbers.length > 0) {
        rec.setValue({ fieldId: 'custrecord_ft_truck', value: group.truckNumbers.join(',') });
    }
    rec.setValue({ fieldId: 'custrecord_po_numbers', value: group.poNumbers.join(',') });
    rec.setValue({ fieldId: 'custrecord_ft_order_quantity', value: group.orderQty });
    rec.setValue({ fieldId: 'custrecord_ft_weight_quantity', value: group.weightQty });
    rec.setValue({ fieldId: 'custrecord_ft_cube_quantity', value: group.cubeQty });
    rec.setValue({ fieldId: 'custrecord_ft_po_quantity_payload', value: JSON.stringify(Object.keys(group.poTotals).map((po) => group.poTotals[po])) });

    if (group.mabd) {
        try {
            const mabdDate = format.parse({ value: group.mabd, type: format.Type.DATE });
            rec.setValue({ fieldId: 'custrecord_ft_mabd', value: mabdDate });
        } catch (e) {
            log.error('createGrsRecord - MABD date parse failed', `value: ${group.mabd} - ${e.message}`);
        }

        if (adjMabd) {
            rec.setValue({ fieldId: 'custrecord_ft_adj_mabd', value: adjMabd });
        } else {
            log.error('createGrsRecord - adjusted MABD calc failed', `value: ${group.mabd}`);
        }
    }

    // Confirm the customer value made it onto the record before save
    if (customerId) {
        const setVal = rec.getValue({ fieldId: GRS_CUSTOMER_FIELD });
        if (String(setVal) !== String(customerId)) {
            log.error('createGrsRecord - customer did not stick before save',
                `externalid ${externalId} | expected ${customerId} | got "${setVal}" - check field id / list type`);
        }
    }

    const newId = rec.save();

    // Confirm it persisted (catches sourcing / workflow / UE clearing it)
    if (customerId) {
        const saved = search.lookupFields({ type: CUSTOM_RECORD_TYPE, id: newId, columns: [GRS_CUSTOMER_FIELD] });
        const savedVal = saved[GRS_CUSTOMER_FIELD] && saved[GRS_CUSTOMER_FIELD][0] ? saved[GRS_CUSTOMER_FIELD][0].value : '';
        if (String(savedVal) !== String(customerId)) {
            log.error('createGrsRecord - customer cleared after save',
                `record ${newId} | expected ${customerId} | saved "${savedVal}" - check field sourcing / workflow / user event`);
        }
    }

    return newId;
}

        function findExistingByExternalId(externalId) {
            let foundId = null;
            const recSearch = search.create({
                type: CUSTOM_RECORD_TYPE,
                filters: [['externalid', 'is', externalId]],
                columns: ['internalid']
            });
            recSearch.run().each((result) => {
                foundId = result.id;
                return false;
            });
            return foundId;
        }

      /** Sets custrecord_ft_customer on an existing GRS record if blank or different. */
        function setCustomerOnExisting(recordId, customerId, externalId) {
    try {
        const current = search.lookupFields({ type: CUSTOM_RECORD_TYPE, id: recordId, columns: [GRS_CUSTOMER_FIELD] });
        const currentVal = current[GRS_CUSTOMER_FIELD] && current[GRS_CUSTOMER_FIELD][0] ? current[GRS_CUSTOMER_FIELD][0].value : '';
        if (String(currentVal) === String(customerId)) return;

        record.submitFields({
            type: CUSTOM_RECORD_TYPE,
            id: recordId,
            values: { [GRS_CUSTOMER_FIELD]: customerId },
            options: { enableSourcing: false, ignoreMandatoryFields: true }
        });
        log.audit('createGrsRecord - customer set on existing record',
            `record ${recordId} (${externalId}) | ${GRS_CUSTOMER_FIELD}: "${currentVal}" -> ${customerId}`);
    } catch (e) {
        log.error('createGrsRecord - failed to set customer on existing record', `record ${recordId}: ${e.message}`);
    }
}

        // =========================================================================
        // Helpers - file
        // =========================================================================

        function writeErrorCsv(originalFile, errorEntries) {
            const headerRow = errorEntries[0].headerRow;
            const csvLines = [];
            csvLines.push([...headerRow, 'ERROR_REASON'].map(csvEscape).join(','));

            errorEntries.forEach((entry) => {
                entry.rows.forEach((row) => {
                    csvLines.push([...row, entry.error].map(csvEscape).join(','));
                });
            });

            const baseName = originalFile.name.replace(/\.csv$/i, '');
            const timestamp = new Date().getTime();
            const errorFileName = `ERROR_${baseName}_${timestamp}.csv`;

            const errFile = file.create({
                name: errorFileName,
                fileType: file.Type.CSV,
                contents: csvLines.join('\n'),
                folder: ERROR_FOLDER_ID
            });
            errFile.save();

            log.error(`writeErrorCsv - ${errorEntries.length} failed group(s) written`, errorFileName);
        }

        function csvEscape(value) {
            const str = String(value == null ? '' : value);
            if (str.indexOf(',') !== -1 || str.indexOf('"') !== -1 || str.indexOf('\n') !== -1) {
                return `"${str.replace(/"/g, '""')}"`;
            }
            return str;
        }

        function moveFile(fileObj, destFolderId) {
            fileObj.folder = destFolderId;
            fileObj.save();
        }

        return { getInputData, map, reduce, summarize };
    });