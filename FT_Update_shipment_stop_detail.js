/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/file', 'N/search'], (file, search) => {

    const FILE_ID = 2777973;

    // 0 = Process all Shipment Reference IDs
    const TEST_LIMIT = 0;

    const getInputData = () => {

        const csv = file.load({ id: FILE_ID }).getContents();

        const refs = [...new Set(
            csv.replace(/^\uFEFF/, '')
                .split(/\r?\n/)
                .map(v => v.trim().replace(/^"|"$/g, ''))
                .filter(v =>
                    v &&
                    v !== 'custrecord_ft_ship_refid'
                )
        )];

        return TEST_LIMIT > 0
            ? refs.slice(0, TEST_LIMIT)
            : refs;
    };

    const map = context => {

        const refId = JSON.parse(context.value);

        const soSearch = search.create({
            type: search.Type.SALES_ORDER,
            filters: [
                ['mainline', 'is', 'T'],
                'AND',
                ['custbody_ft_related_stop_details',
                    'contains', refId]
            ],
            columns: [
                search.createColumn({ name: 'internalid' })
            ]
        });

        // Get all matching Sales Orders for this reference.
        soSearch.run().each(result => {

            const soId = result.getValue({
                name: 'internalid'
            });

            if (soId) {
                context.write({
                    key: String(soId),
                    value: '1'
                });
            }

            return true;
        });
    };

    const reduce = context => {

        // Each unique Sales Order ID appears only once.
        context.write({
            key: context.key,
            value: context.key
        });
    };

    const summarize = summary => {

        // Do not generate an incomplete file if a search failed.
        const errors = [];

        if (summary.inputSummary.error) {
            errors.push(summary.inputSummary.error);
        }

        summary.mapSummary.errors.iterator().each(
            (key, error) => {
                errors.push(key + ': ' + error);
                return true;
            }
        );

        summary.reduceSummary.errors.iterator().each(
            (key, error) => {
                errors.push(key + ': ' + error);
                return true;
            }
        );

        if (errors.length) {
            throw new Error(
                'Processing failed: ' + errors.slice(0, 5).join('; ')
            );
        }

        const uniqueIds = [];

        summary.output.iterator().each((key, value) => {
            uniqueIds.push(String(key));
            return true;
        });

        uniqueIds.sort((a, b) => Number(a) - Number(b));

        // Create final CSV containing unique Sales Order IDs.
        const csvContent = [
            'Sales Order Internal ID',
            ...uniqueIds
        ].join('\n');

        const sourceFile = file.load({ id: FILE_ID });

        const outputFile = file.create({
            name: 'Unique_Sales_Order_IDs_' +
                new Date().toISOString()
                    .replace(/[:.]/g, '-') + '.csv',
            fileType: file.Type.CSV,
            contents: csvContent,
            folder: sourceFile.folder
        });

        outputFile.save();
    };

    return {
        getInputData,
        map,
        reduce,
        summarize
    };
});
