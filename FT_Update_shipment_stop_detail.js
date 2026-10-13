/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], (search, record, log) => {

    const PO_SEARCH = 'customsearch3951';
    const STOP_SEARCH = 'customsearch_ft_finding_old_stops';
    const TARGET = 'custbody_ft_related_stop_details';
    const UPDATE_CHECKBOX = 'custbody_ft_update_stops';

    // 2 = Update first 2 POs for testing.
    // 0 = Process ALL POs returned by Search 1.
    const PO_LIMIT = 2;

    const FIELDS = [
        'name',
        'custrecord_ft_ship_type',
        'custrecord_ft_ship_seq',
        'custrecord_ft_ship_loccode',
        'custrecord_ft_ship_locname',
        'custrecord_ft_ship_add1',
        'custrecord_ft_ship_add2',
        'custrecord_ft_ship_city',
        'custrecord_ft_ship_state',
        'custrecord_ft_ship_zip',
        'custrecord_ft_ship_country',
        'custrecord_ft_ship_plandate',
        'custrecord_ft_ship_actualdate',
        'custrecord_ft_ship_appt',
        'custrecord_ft_ship_latdeg',
        'custrecord_ft_ship_longdeg',
        'custrecord_ft_ship_latdirect',
        'custrecord_ft_longdirection',
        'custrecord_ft_ship_refid',
        'custrecord_ft_shipstop_pallcount',
        'custrecord_ft_stop_weight',
        'custrecord_ft_stop_linearfeet',
        'custrecord_ft_stop_casescount',
        'custrecord_ft_shipstop_apptime',
        'custrecord_ft_stop_plantime',
        'custrecord_ft_stop_actualtime',
        'custrecord_ft_shipstop_latereason',
        'custrecord_ft_shipstop_apptnum'
    ];

    const NUMBER_FIELDS = [
        'custrecord_ft_shipstop_pallcount',
        'custrecord_ft_stop_weight',
        'custrecord_ft_stop_linearfeet',
        'custrecord_ft_stop_casescount'
    ];

    const DATE_FIELDS = [
        'custrecord_ft_ship_plandate',
        'custrecord_ft_ship_actualdate',
        'custrecord_ft_ship_appt'
    ];

    const TIME_FIELDS = [
        'custrecord_ft_shipstop_apptime',
        'custrecord_ft_stop_plantime',
        'custrecord_ft_stop_actualtime'
    ];

    const DATE_TIME_MAP = {
        custrecord_ft_ship_plandate: 'custrecord_ft_stop_plantime',
        custrecord_ft_ship_actualdate: 'custrecord_ft_stop_actualtime',
        custrecord_ft_ship_appt: 'custrecord_ft_shipstop_apptime'
    };

    const formatDate = value => {
        const text = String(value || '').trim();
        const m = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);

        return m
            ? m[1].padStart(2, '0') + '/' +
              m[2].padStart(2, '0') + '/' + m[3]
            : text;
    };

    const formatTime = value => {
        const text = String(value || '').trim();
        if (!text) return '';

        const m = text.match(
            /^(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm)?$/i
        );

        if (!m) return '';

        let hour = Number(m[1]);
        const period = (m[3] || '').toLowerCase();

        if (period === 'pm' && hour < 12) hour += 12;
        if (period === 'am' && hour === 12) hour = 0;

        if (hour > 23 || Number(m[2]) > 59) return '';

        return hour + ':' + m[2];
    };

    const getInputData = () => {
        const s = search.load({ id: PO_SEARCH });

        if (!s.columns.some(c =>
            c.name === 'internalid' &&
            !c.join &&
            c.summary === search.Summary.GROUP
        )) {
            s.columns = [
                ...s.columns,
                search.createColumn({
                    name: 'internalid',
                    summary: search.Summary.GROUP
                })
            ];
        }

        if (PO_LIMIT === 0) {
            log.audit('START - ALL POs', {
                searchId: PO_SEARCH
            });
            return s;
        }

        const results = s.run().getRange({
            start: 0,
            end: PO_LIMIT
        });

        const input = results.map(r => ({
            poId: r.getValue({
                name: 'internalid',
                summary: search.Summary.GROUP
            }),
            docNum: r.getValue({
                name: 'tranid',
                summary: search.Summary.GROUP
            })
        }));

        log.audit('TEST - POs Selected', {
            count: input.length,
            input
        });

        return input;
    };

    const map = context => {
        let poId, docNum;

        const report = (status, details) => {
            context.write({
                key: status,
                value: JSON.stringify(details)
            });
        };

        try {
            const data = JSON.parse(context.value);

            if (PO_LIMIT > 0) {
                poId = data.poId;
                docNum = data.docNum;
            } else {
                const values = data.values || {};

                poId = values['GROUP(internalid)'];
                docNum = values['GROUP(tranid)'];

                if (poId && typeof poId === 'object') {
                    poId = poId.value;
                }
                if (docNum && typeof docNum === 'object') {
                    docNum = docNum.value;
                }
            }

            if (!poId || !docNum) {
                log.error('SKIPPED - Missing PO Details', data);
                report('SKIPPED', { poId, docNum });
                return;
            }

            // SEARCH 2: Group by Sequence, MAX Internal ID.
            const oldSearch = search.load({ id: STOP_SEARCH });

            oldSearch.filterExpression = [
                ['systemnotes.context', 'anyof', 'MPR'],
                'AND',
                ['systemnotes.newvalue', 'startswith', String(docNum)]
            ];

            const seqCol = search.createColumn({
                name: 'custrecord_ft_ship_seq',
                summary: search.Summary.GROUP
            });

            const idCol = search.createColumn({
                name: 'internalid',
                summary: search.Summary.MAX
            });

            oldSearch.columns = [seqCol, idCol];

            const oldResults = oldSearch.run().getRange({
                start: 0,
                end: 1000
            });

            const selectedStops = oldResults.map(r => ({
                sequence: String(r.getValue(seqCol) || ''),
                internalid: String(r.getValue(idCol) || '')
            })).sort((a, b) =>
                Number(a.sequence) - Number(b.sequence)
            );

            const stopIds = selectedStops.map(s => s.internalid);

            log.audit('Historical Stops - Selected Sequences', {
                poId,
                docNum,
                selectedStops
            });

            if (
                selectedStops.length < 2 ||
                selectedStops.length >= 1000 ||
                selectedStops.some(s =>
                    !s.internalid ||
                    !Number.isInteger(Number(s.sequence)) ||
                    Number(s.sequence) < 1
                ) ||
                new Set(stopIds).size !== stopIds.length
            ) {
                log.audit('SKIPPED - Invalid Sequence Data', {
                    poId, docNum, selectedStops
                });

                report('SKIPPED', {
                    poId,
                    docNum,
                    reason: 'Invalid Sequence Data'
                });
                return;
            }

            // SEARCH 3: Retrieve selected Shipment Stop records.
            const columns = FIELDS.map(id =>
                search.createColumn({ name: id })
            );

            const stopResults = search.create({
                type: 'customrecord_ft_ship_stop',
                filters: [
                    ['internalid', 'anyof', stopIds]
                ],
                columns
            }).run().getRange({
                start: 0,
                end: 1000
            });

            if (stopResults.length !== stopIds.length) {
                log.audit('SKIPPED - Missing Stop Records', {
                    poId, docNum, stopIds
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: 'Missing Stop Records'
                });
                return;
            }

            const stops = stopResults.map(r => {
                const obj = { internalid: String(r.id) };

                FIELDS.forEach((field, i) => {
                    obj[field] = r.getValue(columns[i]);
                });

                return obj;
            });

            // Validate selected records match grouped Sequences.
            const invalidStops = stops.some(s => {
                const selected = selectedStops.find(x =>
                    x.internalid === s.internalid
                );

                const seq = Number(s.custrecord_ft_ship_seq);
                const type = String(s.custrecord_ft_ship_type || '');
                const ref = String(s.custrecord_ft_ship_refid || '').trim();

                return !selected ||
                    String(selected.sequence) !== String(seq) ||
                    !ref.startsWith(String(docNum) + '_') ||
                    !/_(\d+)$/.test(ref) ||
                    (seq === 1 && type !== '2') ||
                    (seq > 1 && type !== '1');
            });

            if (invalidStops) {
                log.audit('SKIPPED - Invalid Stop Data', {
                    poId,
                    docNum,
                    selectedStops,
                    records: stops.map(s => ({
                        id: s.internalid,
                        seq: s.custrecord_ft_ship_seq,
                        type: s.custrecord_ft_ship_type,
                        ref: s.custrecord_ft_ship_refid
                    }))
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: 'Invalid Sequence, Type or Reference ID'
                });
                return;
            }

            // Read current PO JSON and checkbox.
            const po = record.load({
                type: record.Type.PURCHASE_ORDER,
                id: poId,
                isDynamic: false
            });

            const oldValue = po.getValue({
                fieldId: TARGET
            }) || '';

            const oldCheckbox = po.getValue({
                fieldId: UPDATE_CHECKBOX
            });

            let existing = [];

            if (oldValue) {
                try {
                    existing = JSON.parse(oldValue);

                    if (!Array.isArray(existing)) {
                        throw new Error('Existing JSON is not an array');
                    }
                } catch (e) {
                    log.error('SKIPPED - Invalid Existing JSON', {
                        poId, docNum, error: e.message
                    });
                    report('SKIPPED', {
                        poId, docNum,
                        reason: 'Invalid Existing JSON'
                    });
                    return;
                }
            }

            const warnings = [];

            // Build one JSON object per unique Sequence.
            const payload = stops.map(stop => {
                const seq = String(stop.custrecord_ft_ship_seq);

                const previous = existing.find(p =>
                    p &&
                    String(p.custrecord_ft_ship_seq) === seq
                ) || {};

                const obj = {};

                FIELDS.forEach(field => {
                    let value = stop[field];

                    // Preserve existing Location Code if missing.
                    if (
                        field === 'custrecord_ft_ship_loccode' &&
                        (value === null ||
                         value === undefined ||
                         value === '')
                    ) {
                        value = previous[field] || '';
                    }

                    // Always include empty listed fields.
                    if (
                        value === null ||
                        value === undefined ||
                        value === ''
                    ) {
                        obj[field] = '';
                        return;
                    }

                    if (NUMBER_FIELDS.includes(field)) {
                        const num = Number(value);

                        if (Number.isFinite(num)) {
                            obj[field] = num;
                        } else {
                            obj[field] = '';
                            warnings.push(
                                'Seq ' + seq + ' Invalid Number: ' + field
                            );
                        }

                    } else if (TIME_FIELDS.includes(field)) {
                        obj[field] = formatTime(value);

                    } else if (DATE_FIELDS.includes(field)) {
                        const date = formatDate(value);
                        const previousValue = String(
                            previous[field] || ''
                        ).trim();

                        const previousDate = formatDate(previousValue);

                        const dateTimeMatch = previousValue.match(
                            /^\d{1,2}\/\d{1,2}\/\d{4}\s+(\d{1,2}:\d{2})$/
                        );

                        if (
                            date &&
                            date === previousDate &&
                            dateTimeMatch
                        ) {
                            obj[field] =
                                date + ' ' + dateTimeMatch[1];
                        } else {
                            // New Sequence: reconstruct from Date + Time.
                            const timeField = DATE_TIME_MAP[field];
                            const time = formatTime(stop[timeField]);

                            if (date && time) {
                                obj[field] = date + ' ' +
                                    time.padStart(5, '0');
                            } else {
                                obj[field] = date || '';

                                if (date) {
                                    warnings.push(
                                        'Seq ' + seq +
                                        ' Missing Timestamp: ' + field
                                    );
                                }
                            }
                        }

                    } else {
                        obj[field] = String(value);
                    }

                    // Derive Stop ID from Shipment Reference ID.
                    if (field === 'custrecord_ft_ship_refid') {
                        const match = String(value).match(/_(\d+)$/);

                        obj.custrecord_ft_stopID =
                            match ? match[1] : '';
                    }
                });

                return obj;
            });

            payload.sort((a, b) =>
                Number(a.custrecord_ft_ship_seq) -
                Number(b.custrecord_ft_ship_seq)
            );

            const newValue = JSON.stringify(payload);

            if (warnings.length) {
                log.audit('SKIPPED - Payload Warnings', {
                    poId, docNum, stopIds, warnings
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: warnings.join('; ')
                });
                return;
            }

            if (oldValue === newValue && oldCheckbox === true) {
                log.audit('ALREADY UPDATED', { poId, docNum });
                report('UNCHANGED', { poId, docNum });
                return;
            }

            // Update JSON and checkbox together.
            record.submitFields({
                type: record.Type.PURCHASE_ORDER,
                id: poId,
                values: {
                    [TARGET]: newValue,
                    [UPDATE_CHECKBOX]: true
                },
                options: {
                    enableSourcing: false,
                    ignoreMandatoryFields: true
                }
            });

            // Separate log containing all Shipment References.
            log.audit('PO Updated - Shipment Reference IDs', {
                poId,
                docNum,
                references: payload.map(p => ({
                    sequence: p.custrecord_ft_ship_seq,
                    referenceId: p.custrecord_ft_ship_refid
                }))
            });

            log.audit('PO UPDATED SUCCESSFULLY', {
                poId,
                docNum,
                stopIds,
                sequenceCount: payload.length,
                oldCheckbox,
                newCheckbox: true,
                oldValue,
                newValue
            });

            report('UPDATED', {
                poId,
                docNum,
                sequenceCount: payload.length,
                stopIds
            });

        } catch (e) {
            log.error('PO Processing Error', {
                poId,
                docNum,
                name: e.name,
                message: e.message,
                stack: e.stack
            });

            report('FAILED', {
                poId,
                docNum,
                error: e.message
            });
        }
    };

    const summarize = summary => {
        const counts = {
            UPDATED: 0,
            SKIPPED: 0,
            UNCHANGED: 0,
            FAILED: 0
        };

        summary.output.iterator().each((key, value) => {
            counts[key] = (counts[key] || 0) + 1;

            if (key !== 'UPDATED') {
                log.audit('PO Result - ' + key, JSON.parse(value));
            }
            return true;
        });

        summary.mapSummary.errors.iterator().each((key, error) => {
            log.error('Uncaught Map Error - ' + key, error);
            return true;
        });

        log.audit('FINAL MR SUMMARY', {
            mode: PO_LIMIT === 0 ? 'ALL POs' : 'TEST ' + PO_LIMIT,
            counts,
            usage: summary.usage,
            yields: summary.yields,
            seconds: summary.seconds,
            inputError: summary.inputSummary.error || ''
        });
    };

    return { getInputData, map, summarize };
});
