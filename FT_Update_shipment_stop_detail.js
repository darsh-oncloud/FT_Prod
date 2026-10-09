/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], (search, record, log) => {

    const PO_SEARCH = 'customsearch3951';
    const STOP_SEARCH = 'customsearch_ft_finding_old_stops';
    const TARGET = 'custbody_ft_related_stop_details';

    // TEST MODE: Process only first PO, no updates.
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
        'custrecord_ft_shipstop_apptnum',
        'custrecord_ft_shipstop_latereason'
    ];

    const getInputData = () => {
        const s = search.load({ id: PO_SEARCH });

        // Add grouped PO Internal ID.
        s.columns = [
            ...s.columns.filter(c =>
                !(c.name === 'internalid' && !c.join &&
                  c.summary === search.Summary.GROUP)
            ),
            search.createColumn({
                name: 'internalid',
                summary: search.Summary.GROUP
            })
        ];

        const results = s.run().getRange({
            start: 0,
            end: TEST_MODE ? 1 : 1000
        });

        log.audit('PO Search Results', {
            mode: TEST_MODE ? 'DRY RUN - ONE PO' : 'LIVE',
            selectedCount: results.length
        });

        return results.map(r => ({
            poId: r.getValue({
                name: 'internalid',
                summary: search.Summary.GROUP
            }),
            docNum: r.getValue({
                name: 'tranid',
                summary: search.Summary.GROUP
            })
        }));
    };

    const map = context => {
        const { poId, docNum } = JSON.parse(context.value);

        try {
            log.audit('1 - Processing PO', { poId, docNum });

            if (!poId || !docNum) {
                log.error('SKIPPED - Missing PO Details', {
                    poId, docNum
                });
                return;
            }

            // SEARCH 2: Historical Shipment Stops.
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

            log.audit('2 - Historical Stops Found', {
                docNum,
                count: oldResults.length,
                stopIds
            });

            if (oldResults.length !== 2 ||
                new Set(stopIds.map(String)).size !== 2 ||
                stopIds.some(id => !id)) {

                log.audit('SKIPPED - Expected 2 Unique Stops', {
                    docNum,
                    count: oldResults.length,
                    stopIds
                });
                return;
            }

            // SEARCH 3: Get actual Shipment Stop values.
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
                log.audit('SKIPPED - Stop Records Not Found', {
                    docNum, stopIds
                });
                return;
            }

            const payload = stopResults.map(r => {
                const obj = {};

                FIELDS.forEach((id, i) => {
                    const value = r.getValue(columns[i]);

                    if (value !== null &&
                        value !== undefined &&
                        value !== '') {
                        obj[id] = value;
                    } else if (id === 'custrecord_ft_ship_add2') {
                        obj[id] = '';
                    }
                });

                return obj;
            });

            const types = payload.map(p =>
                String(p.custrecord_ft_ship_type || '')
            );

            const refs = payload.map(p =>
                p.custrecord_ft_ship_refid || ''
            );

            log.audit('3 - Stop Validation', {
                docNum,
                stopIds,
                types,
                refs
            });

            // Exactly one Pickup (2), one Drop Off (1).
            if (!types.includes('1') ||
                !types.includes('2') ||
                refs.some(ref => !String(ref).trim())) {

                log.audit('SKIPPED - Invalid Stops', {
                    docNum,
                    stopIds,
                    types,
                    refs,
                    reason: 'Invalid Pickup/Drop Off or missing Stop Reference ID'
                });
                return;
            }

            // Pickup first, Drop Off second.
            payload.sort((a, b) =>
                Number(b.custrecord_ft_ship_type) -
                Number(a.custrecord_ft_ship_type)
            );

            const newValue = JSON.stringify(payload);

            // Read existing PO field for comparison.
            const po = record.load({
                type: record.Type.PURCHASE_ORDER,
                id: poId,
                isDynamic: false
            });

            const oldValue = po.getValue({
                fieldId: TARGET
            });

            log.audit('4 - Existing PO Payload', {
                poId,
                docNum,
                oldValue
            });

            log.audit('5 - NEW Corrected JSON Payload', {
                poId,
                docNum,
                stopIds,
                newValue
            });

            if (TEST_MODE) {
                log.audit('6 - TEST SUCCESS - NO UPDATE', {
                    poId,
                    docNum,
                    pickupId: stopIds[types.indexOf('2')],
                    dropoffId: stopIds[types.indexOf('1')],
                    payloadLength: newValue.length,
                    message: 'Validated JSON. PO NOT UPDATED.'
                });
                return;
            }

            // LIVE MODE: Update the PO field.
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

            log.audit('6 - PO Updated', {
                poId,
                docNum,
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
        log.audit('Map Reduce Completed', {
            mode: TEST_MODE ? 'TEST - NO UPDATE' : 'LIVE',
            inputError: summary.inputSummary.error || '',
            mapErrors: [...summary.mapSummary.errors.iterator()]
        });
    };

    return { getInputData, map, summarize };
});
