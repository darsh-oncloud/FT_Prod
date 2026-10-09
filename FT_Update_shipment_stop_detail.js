/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], (search, record, log) => {

    const PO_SEARCH = 'customsearch3951';
    const STOP_SEARCH = 'customsearch_ft_finding_old_stops';
    const TARGET = 'custbody_ft_related_stop_details';

    // Only first PO from Saved Search 1.
    const ONE_PO_ONLY = true;

    // Actual update enabled.
    const UPDATE_PO = true;

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

        // Ensure PO Internal ID is available.
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

        if (!ONE_PO_ONLY) return s;

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

        log.audit('1 - PO Selected', {
            count: input.length,
            input
        });

        return input;
    };

    const map = context => {

        let poId, docNum;

        try {

            const data = JSON.parse(context.value);

            if (ONE_PO_ONLY) {
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
                return;
            }

            log.audit('2 - Processing PO', {
                poId,
                docNum
            });

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

            log.audit('3 - Historical Stops Found', {
                docNum,
                count: oldResults.length,
                stopIds
            });

            if (
                oldResults.length !== 2 ||
                stopIds.some(id => !id) ||
                new Set(stopIds.map(String)).size !== 2
            ) {
                log.audit('SKIPPED - Invalid Stop Count', {
                    docNum,
                    stopIds,
                    count: oldResults.length
                });
                return;
            }

            // SEARCH 3 - Actual Shipment Stop fields.
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
                    docNum,
                    stopIds
                });
                return;
            }

            const stops = stopResults.map(r => {

                const obj = {
                    internalid: r.id
                };

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

            log.audit('4 - Stop Validation', {
                docNum,
                stopIds,
                types,
                refs
            });

            // Require exactly one Pickup and one Drop Off.
            // Both reference IDs must be populated.
            if (
                !types.includes('1') ||
                !types.includes('2') ||
                refs.some(ref =>
                    !ref || !ref.startsWith(String(docNum) + '_')
                )
            ) {
                log.audit('SKIPPED - Invalid Stop Data', {
                    docNum,
                    stopIds,
                    types,
                    refs
                });
                return;
            }

            // Read PO existing JSON.
            const po = record.load({
                type: record.Type.PURCHASE_ORDER,
                id: poId,
                isDynamic: false
            });

            const oldValue = po.getValue({
                fieldId: TARGET
            }) || '';

            log.audit('5 - Existing PO Payload', {
                poId,
                docNum,
                oldValue
            });

            let existing = [];

            if (oldValue) {
                try {
                    existing = JSON.parse(oldValue);

                    if (!Array.isArray(existing)) {
                        throw new Error(
                            'Existing payload must be an array'
                        );
                    }

                } catch (e) {
                    log.error('SKIPPED - Invalid Existing JSON', {
                        poId,
                        docNum,
                        error: e.message
                    });
                    return;
                }
            }

            const warnings = [];

            // Build full JSON from Shipment Stop records.
            const payload = stops.map(stop => {

                const type = String(
                    stop.custrecord_ft_ship_type
                );

                const previous = existing.find(p =>
                    String(p.custrecord_ft_ship_type) === type
                ) || {};

                const obj = {};

                FIELDS.forEach(field => {

                    let value = stop[field];

                    // Preserve Location Code if missing on stop.
                    if (
                        field === 'custrecord_ft_ship_loccode' &&
                        (value === null ||
                         value === undefined ||
                         value === '')
                    ) {
                        value = previous[field] || '';
                    }

                    // Always include empty payload fields.
                    if (
                        value === null ||
                        value === undefined ||
                        value === ''
                    ) {
                        obj[field] = '';
                        return;
                    }

                    // Numeric JSON fields.
                    if (NUMBER_FIELDS.includes(field)) {

                        const num = Number(value);

                        if (Number.isFinite(num)) {
                            obj[field] = num;
                        } else {
                            obj[field] = '';
                            warnings.push(
                                type + ' - Invalid number: ' + field
                            );
                        }

                    } else if (TIME_FIELDS.includes(field)) {

                        obj[field] = formatTime(value);

                    } else if (DATE_FIELDS.includes(field)) {

                        const date = formatDate(value);
                        const previousValue = String(
                            previous[field] || ''
                        ).trim();

                        const previousDate =
                            formatDate(previousValue);

                        const dateTimeMatch = previousValue.match(
                            /^\d{1,2}\/\d{1,2}\/\d{4}\s+(\d{1,2}:\d{2})$/
                        );

                        // Preserve complete original timestamp
                        // only if the record dates match.
                        if (
                            date &&
                            date === previousDate &&
                            dateTimeMatch
                        ) {
                            obj[field] =
                                date + ' ' + dateTimeMatch[1];

                        } else {
                            obj[field] = date;

                            warnings.push(
                                type + ' - Missing matching timestamp: ' +
                                field
                            );
                        }

                    } else {

                        obj[field] = String(value);
                    }

                    // Add Stop ID immediately after reference ID.
                    if (field === 'custrecord_ft_ship_refid') {

                        const match = String(value).match(
                            /_(\d+)$/
                        );

                        obj.custrecord_ft_stopID =
                            match ? match[1] : '';

                        if (!match) {
                            warnings.push(
                                type + ' - Invalid Stop Reference ID'
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

            log.audit('6 - NEW Corrected JSON Payload', {
                poId,
                docNum,
                stopIds,
                newValue
            });

            log.audit('7 - Payload Validation', {
                poId,
                docNum,
                pickupRef:
                    payload[0].custrecord_ft_ship_refid,
                dropoffRef:
                    payload[1].custrecord_ft_ship_refid,
                pickupStopID:
                    payload[0].custrecord_ft_stopID,
                dropoffStopID:
                    payload[1].custrecord_ft_stopID,
                payloadLength: newValue.length,
                warnings
            });

            // Do not write a potentially incomplete payload.
            if (warnings.length) {
                log.audit('SKIPPED - Payload Warnings', {
                    poId,
                    docNum,
                    warnings,
                    message: 'PO NOT UPDATED'
                });
                return;
            }

            if (!UPDATE_PO) {
                log.audit('TEST COMPLETE - NO UPDATE', {
                    poId,
                    docNum,
                    newValue
                });
                return;
            }

            // UPDATE PURCHASE ORDER.
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

            log.audit('8 - PO UPDATED SUCCESSFULLY', {
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
                message: e.message,
                stack: e.stack
            });
        }
    };

    const summarize = summary => {

        let errors = 0;

        summary.mapSummary.errors.iterator().each(
            (key, error) => {
                errors++;

                log.error('Map Error - ' + key, error);
                return true;
            }
        );

        log.audit('Map Reduce Completed', {
            onePOOnly: ONE_PO_ONLY,
            updateEnabled: UPDATE_PO,
            inputError: summary.inputSummary.error || '',
            mapErrors: errors,
            usage: summary.usage,
            yields: summary.yields
        });
    };

    return {
        getInputData,
        map,
        summarize
    };
});
