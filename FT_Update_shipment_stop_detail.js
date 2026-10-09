/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], (search, record, log) => {

    const PO_SEARCH = 'customsearch3951';
    const STOP_SEARCH = 'customsearch_ft_finding_old_stops';
    const TARGET = 'custbody_ft_related_stop_details';

    const TEST_MODE = true;

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

    // Format NetSuite time as 24-hour H:mm.
    const formatTime = value => {
        const text = String(value || '').trim();
        if (!text) return '';

        const m = text.match(/^(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm)?$/i);
        if (!m) return text;

        let hour = Number(m[1]);
        const minute = m[2];
        const period = (m[3] || '').toLowerCase();

        if (period === 'pm' && hour < 12) hour += 12;
        if (period === 'am' && hour === 12) hour = 0;

        return hour + ':' + minute;
    };

    const formatDate = value => {
        const text = String(value || '').trim();
        const m = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);

        if (!m) return text;

        return m[1].padStart(2, '0') + '/' +
            m[2].padStart(2, '0') + '/' + m[3];
    };

    const getInputData = () => {
        const s = search.load({ id: PO_SEARCH });

        // Add PO ID to existing summary search.
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

        if (!TEST_MODE) return s;

        // Test exactly one PO.
        const results = s.run().getRange({
            start: 0,
            end: 1
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

        log.audit('TEST INPUT - ONE PO', input);

        return input;
    };

    const map = context => {
        let poId, docNum;

        try {
            const input = JSON.parse(context.value);

            if (TEST_MODE) {
                poId = input.poId;
                docNum = input.docNum;
            } else {
                // Saved search result from live getInputData.
                const values = input.values || {};
                poId = values['GROUP(internalid)'];
                docNum = values['GROUP(tranid)'];

                if (poId && typeof poId === 'object') poId = poId.value;
                if (docNum && typeof docNum === 'object') docNum = docNum.value;
            }

            if (!poId || !docNum) {
                log.error('SKIPPED - Missing PO ID/Number', input);
                return;
            }

            log.audit('1 - Processing PO', { poId, docNum });

            // SEARCH 2: Use PO number in saved search.
            const oldSearch = search.load({ id: STOP_SEARCH });

            oldSearch.filterExpression = [
                ['systemnotes.context', 'anyof', 'MPR'],
                'AND',
                ['systemnotes.newvalue', 'startswith', String(docNum)]
            ];

            const oldResults = oldSearch.run().getRange({
                start: 0,
                end: 3
            });

            const stopIds = oldResults.map(r => r.getValue({
                name: 'internalid',
                summary: search.Summary.MAX
            }));

            log.audit('2 - Historical Stops', {
                docNum,
                count: oldResults.length,
                stopIds
            });

            if (oldResults.length !== 2 ||
                stopIds.some(id => !id) ||
                new Set(stopIds.map(String)).size !== 2) {

                log.audit('SKIPPED - Invalid Stop Count', {
                    docNum, stopIds,
                    count: oldResults.length
                });
                return;
            }

            // SEARCH 3: Retrieve actual Shipment Stop data.
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
                end: 3
            });

            if (stopResults.length !== 2) {
                log.audit('SKIPPED - Missing Stop Records', {
                    docNum, stopIds
                });
                return;
            }

            const stops = stopResults.map(r => {
                const data = {
                    internalid: r.id
                };

                FIELDS.forEach((field, i) => {
                    data[field] = r.getValue(columns[i]);
                });

                return data;
            });

            const types = stops.map(s =>
                String(s.custrecord_ft_ship_type || '')
            );

            const refs = stops.map(s =>
                String(s.custrecord_ft_ship_refid || '').trim()
            );

            log.audit('3 - Stop Validation', {
                docNum,
                stopIds,
                types,
                refs
            });

            if (!types.includes('1') ||
                !types.includes('2') ||
                refs.some(ref => !ref)) {

                log.audit('SKIPPED - Invalid Stops', {
                    docNum,
                    stopIds,
                    types,
                    refs
                });
                return;
            }

            // Read current PO JSON.
            const po = record.load({
                type: record.Type.PURCHASE_ORDER,
                id: poId,
                isDynamic: false
            });

            const oldValue = po.getValue({
                fieldId: TARGET
            }) || '';

            log.audit('4 - Existing PO JSON', {
                poId, docNum, oldValue
            });

            let existing = [];

            if (oldValue) {
                try {
                    existing = JSON.parse(oldValue);
                    if (!Array.isArray(existing)) {
                        throw new Error('Existing payload is not an array');
                    }
                } catch (e) {
                    log.error('SKIPPED - Invalid Existing JSON', {
                        docNum,
                        message: e.message
                    });
                    return;
                }
            }

            // Build JSON using actual stop records.
            const warnings = [];

            const payload = stops.map(stop => {
                const type = String(stop.custrecord_ft_ship_type);

                // Match existing data by Pickup/Drop Off.
                const previous = existing.find(p =>
                    String(p.custrecord_ft_ship_type) === type
                ) || {};

                const obj = {};

                FIELDS.forEach(field => {
                    let value = stop[field];

                    if (value === null ||
                        value === undefined ||
                        value === '') {

                        // Preserve old value when actual field is empty.
                        value = previous[field];
                    }

                    if (value === null ||
                        value === undefined ||
                        value === '') {

                        if (field === 'custrecord_ft_ship_add2') {
                            obj[field] = '';
                        }
                        return;
                    }

                    if (NUMBER_FIELDS.includes(field)) {
                        const num = Number(value);
                        if (Number.isFinite(num)) {
                            obj[field] = num;
                        } else {
                            warnings.push(type + ': invalid number ' + field);
                        }
                    } else if (TIME_FIELDS.includes(field)) {
                        obj[field] = formatTime(value);
                    } else if (DATE_FIELDS.includes(field)) {
                        const recordDate = formatDate(value);
                        const oldDateTime = String(previous[field] || '');
                        const oldDate = formatDate(oldDateTime);

                        // Preserve existing hour/minute if date agrees.
                        if (recordDate &&
                            oldDate === recordDate &&
                            /^\d{1,2}\/\d{1,2}\/\d{4}\s+\d{1,2}:\d{2}$/.test(oldDateTime)) {

                            obj[field] = recordDate + ' ' +
                                oldDateTime.trim().split(/\s+/)[1];

                        } else {
                            obj[field] = recordDate;
                            warnings.push(
                                type + ': Full timestamp unavailable for ' +
                                field
                            );
                        }
                    } else {
                        obj[field] = String(value);
                    }
                });

                // Always use actual reference ID from Shipment Stop.
                obj.custrecord_ft_ship_refid =
                    String(stop.custrecord_ft_ship_refid).trim();

                // Derive Stop ID from trailing reference ID segment.
                const match = obj.custrecord_ft_ship_refid.match(/_(\d+)$/);

                if (match) {
                    obj.custrecord_ft_stopID = match[1];
                } else {
                    warnings.push(type + ': Cannot derive Stop ID');
                }

                return obj;
            });

            // Pickup first, then Drop Off.
            payload.sort((a, b) =>
                Number(b.custrecord_ft_ship_type) -
                Number(a.custrecord_ft_ship_type)
            );

            const newValue = JSON.stringify(payload);

            log.audit('5 - CORRECTED JSON PAYLOAD', {
                poId,
                docNum,
                stopIds,
                newValue
            });

            log.audit('6 - Payload Review', {
                docNum,
                pickupRef: payload[0].custrecord_ft_ship_refid,
                dropoffRef: payload[1].custrecord_ft_ship_refid,
                pickupStopID: payload[0].custrecord_ft_stopID,
                dropoffStopID: payload[1].custrecord_ft_stopID,
                payloadLength: newValue.length,
                warnings
            });

            if (TEST_MODE) {
                log.audit('7 - TEST SUCCESS - NO PO UPDATE', {
                    poId,
                    docNum,
                    message: 'JSON generated. Purchase Order not saved.'
                });
                return;
            }

            // In live mode, do not save incomplete timestamps.
            if (warnings.length) {
                log.audit('SKIPPED - Payload Requires Review', {
                    poId, docNum, warnings
                });
                return;
            }

            record.submitFields({
                type: record.Type.PURCHASE_ORDER,
                id: poId,
                values: {
                    [TARGET]: newValue
                },
                options: {
                    enableSourcing: false,
                    ignoreMandatoryFields: true
                }
            });

            log.audit('7 - PO UPDATED SUCCESSFULLY', {
                poId,
                docNum,
                stopIds,
                oldValue,
                newValue
            });

        } catch (e) {
            log.error('PO Processing Error', {
                poId,
                docNum,
                name: e.name,
                message: e.message
            });
        }
    };

    const summarize = summary => {
        let errors = 0;

        summary.mapSummary.errors.iterator().each((key, error) => {
            errors++;
            log.error('Map Error - ' + key, error);
            return true;
        });

        log.audit('Map Reduce Completed', {
            mode: TEST_MODE ? 'TEST - ONE PO' : 'LIVE',
            inputError: summary.inputSummary.error || '',
            mapErrors: errors,
            usage: summary.usage,
            yields: summary.yields
        });
    };

    return { getInputData, map, summarize };
});
