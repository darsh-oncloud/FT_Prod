/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], (search, record, log) => {

    const PO_SEARCH = 'customsearch3951';
    const STOP_SEARCH = 'customsearch_ft_finding_old_stops';

    const TARGET = 'custbody_ft_related_stop_details';
    const UPDATE_CHECKBOX = 'custbody_ft_update_stops';

    // 2 = Test first 2 POs and update them.
    // 0 = Process ALL POs from saved search.
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

        if (!m) return text;

        let hour = Number(m[1]);
        const period = (m[3] || '').toLowerCase();

        if (period === 'pm' && hour < 12) hour += 12;
        if (period === 'am' && hour === 12) hour = 0;

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

        // Production: Return search directly.
        // NetSuite handles paging and Map/Reduce input.
        if (PO_LIMIT === 0) {
            log.audit('START - ALL POs', {
                searchId: PO_SEARCH,
                mode: 'PRODUCTION'
            });
            return s;
        }

        // Test: Only first 2 PO results.
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
            requested: PO_LIMIT,
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

            // SEARCH 2 - Historical Shipment Stops.
            const oldSearch = search.load({
                id: STOP_SEARCH
            });

            oldSearch.filterExpression = [
                ['systemnotes.context', 'anyof', 'MPR'],
                'AND',
                ['systemnotes.newvalue', 'startswith', String(docNum)]
            ];

            const oldResults = oldSearch.run().getRange({
                start: 0,
                end: 3
            });

            const stopIds = oldResults.map(r =>
                r.getValue({
                    name: 'internalid',
                    summary: search.Summary.MAX
                })
            );

            // Require exactly 2 unique Shipment Stops.
            if (
                oldResults.length !== 2 ||
                stopIds.some(id => !id) ||
                new Set(stopIds.map(String)).size !== 2
            ) {
                log.audit('SKIPPED - Invalid Stop Count', {
                    poId, docNum, stopIds,
                    count: oldResults.length
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: 'Invalid Stop Count'
                });
                return;
            }

            // SEARCH 3 - Actual Shipment Stop data.
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
                log.audit('SKIPPED - Missing Stops', {
                    poId, docNum, stopIds
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: 'Missing Stop Records'
                });
                return;
            }

            const stops = stopResults.map(r => {
                const obj = {};

                FIELDS.forEach((field, i) => {
                    obj[field] = r.getValue(columns[i]);
                });

                return obj;
            });

            const types = stops.map(s =>
                String(s.custrecord_ft_ship_type || '')
            );

            const refs = stops.map(s =>
                String(s.custrecord_ft_ship_refid || '').trim()
            );

            // Exactly 1 Pickup and 1 Drop Off,
            // both with valid Shipment Reference IDs.
            if (
                !types.includes('1') ||
                !types.includes('2') ||
                refs.some(ref =>
                    !ref || !ref.startsWith(String(docNum) + '_')
                )
            ) {
                log.audit('SKIPPED - Invalid Stop Data', {
                    poId, docNum, stopIds, types, refs
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: 'Invalid Pickup/Drop Off or Ref ID'
                });
                return;
            }

            // Load existing PO field values.
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
                        poId, docNum,
                        error: e.message
                    });

                    report('SKIPPED', {
                        poId, docNum,
                        reason: 'Invalid Existing JSON'
                    });
                    return;
                }
            }

            const warnings = [];

            // Build corrected JSON.
            const payload = stops.map(stop => {
                const type = String(stop.custrecord_ft_ship_type);

                const previous = existing.find(p =>
                    p &&
                    String(p.custrecord_ft_ship_type) === type
                ) || {};

                const obj = {};

                FIELDS.forEach(field => {
                    let value = stop[field];

                    // Preserve original PO Location Code
                    // when missing on the Shipment Stop.
                    if (
                        field === 'custrecord_ft_ship_loccode' &&
                        (value === null ||
                         value === undefined ||
                         value === '')
                    ) {
                        value = previous[field] || '';
                    }

                    // Include all specified fields,
                    // even when empty.
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
                                type + ' Invalid Number: ' + field
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

                        // Preserve verified original timestamp.
                        if (
                            date &&
                            date === previousDate &&
                            dateTimeMatch
                        ) {
                            obj[field] =
                                date + ' ' + dateTimeMatch[1];
                        } else {
                            obj[field] = date;

                            if (date) {
                                warnings.push(
                                    type + ' Missing Timestamp: ' + field
                                );
                            }
                        }

                    } else {
                        obj[field] = String(value);
                    }

                    // Get Stop ID from reference suffix.
                    if (field === 'custrecord_ft_ship_refid') {
                        const match = String(value).match(/_(\d+)$/);

                        obj.custrecord_ft_stopID =
                            match ? match[1] : '';

                        if (!match) {
                            warnings.push(
                                type + ' Invalid Stop Reference ID'
                            );
                        }
                    }
                });

                return obj;
            });

            // Pickup first, Drop Off second.
            payload.sort((a, b) =>
                Number(b.custrecord_ft_ship_type) -
                Number(a.custrecord_ft_ship_type)
            );

            const newValue = JSON.stringify(payload);

            // Skip if payload has missing required timestamps.
            if (warnings.length) {
                log.audit('SKIPPED - Payload Warnings', {
                    poId,
                    docNum,
                    stopIds,
                    warnings
                });

                report('SKIPPED', {
                    poId, docNum,
                    reason: warnings.join('; ')
                });
                return;
            }

            // Skip if corrected JSON and checkbox already match.
            if (oldValue === newValue && oldCheckbox === true) {
                log.audit('ALREADY UPDATED', {
                    poId, docNum
                });

                report('UNCHANGED', { poId, docNum });
                return;
            }

            // UPDATE JSON + CHECKBOX IN ONE CALL.
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

            log.audit('PO UPDATED SUCCESSFULLY', {
                poId,
                docNum,
                stopIds,
                pickupRef: payload[0].custrecord_ft_ship_refid,
                dropoffRef: payload[1].custrecord_ft_ship_refid,
                oldCheckbox,
                newCheckbox: true,
                oldValue,
                newValue
            });

            report('UPDATED', {
                poId,
                docNum,
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

    // Summarize successful, skipped and failed POs.
    const summarize = summary => {
        const counts = {
            UPDATED: 0,
            SKIPPED: 0,
            UNCHANGED: 0,
            FAILED: 0
        };

        const details = [];

        summary.output.iterator().each((key, value) => {
            counts[key] = (counts[key] || 0) + 1;

            if (key !== 'UPDATED') {
                details.push({
                    status: key,
                    data: JSON.parse(value)
                });
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

        // Log skipped / failed records individually.
        details.forEach(item => {
            log.audit('PO Result - ' + item.status, item.data);
        });
    };

    return {
        getInputData,
        map,
        summarize
    };
});
