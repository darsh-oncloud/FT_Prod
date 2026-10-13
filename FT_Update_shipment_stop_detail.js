/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/file', 'N/search', 'N/log'],
    (file, search, log) => {

    const FILE_ID = 21490546;
    const FOLDER_ID = 2777973;

    // 0 = Process all Shipment Reference IDs
    // 2 = Process first 2 IDs for testing
    const TEST_LIMIT = 0;

    const getInputData = () => {

        const csv = file.load({
            id: FILE_ID
        }).getContents();

        const refs = [...new Set(
            csv.replace(/^\uFEFF/, '')
                .split(/\r?\n/)
                .map(v => v.trim().replace(/^"|"$/g, ''))
                .filter(v =>
                    v &&
                    v !== 'custrecord_ft_ship_refid'
                )
        )];

        const input = TEST_LIMIT > 0
            ? refs.slice(0, TEST_LIMIT)
            : refs;

        log.audit('Shipment References Loaded', {
            fileId: FILE_ID,
            totalReferences: refs.length,
            processing: input.length
        });

        return input;
    };

    const map = context => {

        const refId = String(context.value).trim();

        const soSearch = search.create({
            type: search.Type.SALES_ORDER,
            filters: [
                ['mainline', 'is', 'T'],
                'AND',
                ['custbody_ft_related_stop_details',
                    'contains', refId]
            ],
            columns: [
                search.createColumn({
                    name: 'internalid'
                })
            ]
        });

        // Retrieve matching Sales Order Internal IDs.
        soSearch.run().each(result => {

            const soId = result.getValue({
                name: 'internalid'
            });

            if (soId) {
                context.write({
                    key: String(soId),
                    value: refId
                });
            }

            return true;
        });
    };

    const reduce = context => {

        // Each unique Sales Order ID is processed once.
        context.write({
            key: context.key,
            value: context.key
        });
    };

    const summarize = summary => {

        let errorCount = 0;

        if (summary.inputSummary.error) {
            errorCount++;
            log.error('Input Error', summary.inputSummary.error);
        }

        summary.mapSummary.errors.iterator().each(
            (key, error) => {
                errorCount++;
                log.error('Map Error - ' + key, error);
                return true;
            }
        );

        summary.reduceSummary.errors.iterator().each(
            (key, error) => {
                errorCount++;
                log.error('Reduce Error - ' + key, error);
                return true;
            }
        );

        // Avoid creating incomplete CSV if errors occurred.
        if (errorCount > 0) {
            log.error('CSV NOT CREATED', {
                errorCount,
                reason: 'One or more searches failed'
            });
            return;
        }

        const uniqueIds = [];

        summary.output.iterator().each((key, value) => {
            uniqueIds.push(String(key));
            return true;
        });

        uniqueIds.sort((a, b) => Number(a) - Number(b));

        const csvContent = [
            'Sales Order Internal ID',
            ...uniqueIds
        ].join('\n');

        // Save final CSV in the requested folder.
        const outputFile = file.create({
            name: 'Unique_Sales_Order_IDs_' +
                new Date().toISOString()
                    .replace(/[:.]/g, '-') + '.csv',
            fileType: file.Type.CSV,
            contents: csvContent,
            folder: FOLDER_ID
        });

        const outputFileId = outputFile.save();

        log.audit('FINAL CSV CREATED SUCCESSFULLY', {
            outputFileId,
            folderId: FOLDER_ID,
            totalUniqueSalesOrders: uniqueIds.length,
            fileName: outputFile.name,
            seconds: summary.seconds,
            usage: summary.usage,
            yields: summary.yields
        });
    };

    return {
        getInputData,
        map,
        reduce,
        summarize
    };
});
