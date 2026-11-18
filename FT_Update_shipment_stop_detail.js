/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * Purpose:
 * Find Purchase Order Stop Payload History files
 * where ANY historical payload is missing:
 * custrecord_ft_ship_refid
 *
 * READ ONLY - No record/file updates.
 */
define(['N/search', 'N/file', 'N/log'], function(search, file, log) {

    var FOLDER_ID = 2788915;
    var REQUIRED_FIELD = 'custrecord_ft_ship_refid';

    function getInputData() {

        return search.create({
            type: 'file',
            filters: [
                ['folder', 'anyof', FOLDER_ID],
                'AND',
                ['name', 'startswith', 'purchaseorder_']
            ],
            columns: [
                'internalid',
                'name'
            ]
        });
    }

    function map(context) {

        var fileId = '';
        var fileName = '';

        try {

            var result = JSON.parse(context.value);

            fileId = result.id;
            fileName = result.values.name;

            if (typeof fileName === 'object') {
                fileName = fileName.value || fileName.text || '';
            }

            if (String(fileName).toLowerCase().indexOf('purchaseorder_') !== 0) {
                return;
            }

            var historyFile = file.load({
                id: fileId
            });

            var content = historyFile.getContents();

            var payloads = extractPayloads(content);

            var missingPayloads = [];

            for (var i = 0; i < payloads.length; i++) {

                var payload = payloads[i];

                if (payload.indexOf(REQUIRED_FIELD) === -1) {
                    missingPayloads.push(i + 1);
                }
            }

            var status = 'PASS';

            if (payloads.length === 0) {
                status = 'NO_PAYLOAD_FOUND';

            } else if (missingPayloads.length > 0) {
                status = 'MISSING_REF_ID';

                log.audit({
                    title: 'PO FILE - MISSING SHIPMENT REF ID',
                    details: JSON.stringify({
                        fileId: fileId,
                        fileName: fileName,
                        totalPayloads: payloads.length,
                        missingCount: missingPayloads.length,
                        missingPayloadNumbers: missingPayloads
                    })
                });
            }

            context.write({
                key: status,
                value: JSON.stringify({
                    fileId: fileId,
                    fileName: fileName,
                    totalPayloads: payloads.length,
                    missingPayloads: missingPayloads
                })
            });

        } catch (e) {

            log.error({
                title: 'FILE PROCESSING ERROR - ' + fileId,
                details: e.message
            });

            context.write({
                key: 'ERROR',
                value: JSON.stringify({
                    fileId: fileId,
                    fileName: fileName,
                    error: e.message
                })
            });
        }
    }

    /**
     * Extract separate top-level JSON objects or arrays
     * from a text file containing payload history.
     *
     * Supports nested objects, multiline JSON,
     * quoted strings and escaped characters.
     */
    function extractPayloads(content) {

        var payloads = [];
        var start = -1;
        var stack = [];
        var inString = false;
        var escaped = false;

        for (var i = 0; i < content.length; i++) {

            var ch = content.charAt(i);

            if (start === -1) {

                if (ch === '{' || ch === '[') {
                    start = i;
                    stack = [ch === '{' ? '}' : ']'];
                    inString = false;
                    escaped = false;
                }

                continue;
            }

            if (escaped) {
                escaped = false;
                continue;
            }

            if (ch === '\\' && inString) {
                escaped = true;
                continue;
            }

            if (ch === '"') {
                inString = !inString;
                continue;
            }

            if (inString) {
                continue;
            }

            if (ch === '{') {
                stack.push('}');

            } else if (ch === '[') {
                stack.push(']');

            } else if (ch === '}' || ch === ']') {

                var expected = stack.pop();

                if (expected !== ch) {
                    start = -1;
                    stack = [];
                    continue;
                }

                if (stack.length === 0) {

                    payloads.push(
                        content.substring(start, i + 1)
                    );

                    start = -1;
                }
            }
        }

        return payloads;
    }

    function reduce(context) {
        // No reduce required.
    }

    function summarize(summary) {

        var totalFiles = 0;
        var passedFiles = 0;
        var missingFiles = 0;
        var noPayloadFiles = 0;
        var errorFiles = 0;
        var totalMissingPayloads = 0;

        summary.output.iterator().each(function(key, value) {

            var result = JSON.parse(value);

            totalFiles++;

            if (key === 'PASS') {
                passedFiles++;

            } else if (key === 'MISSING_REF_ID') {

                missingFiles++;
                totalMissingPayloads += result.missingPayloads.length;

            } else if (key === 'NO_PAYLOAD_FOUND') {
                noPayloadFiles++;

            } else if (key === 'ERROR') {
                errorFiles++;
            }

            return true;
        });

        log.audit({
            title: 'FINAL PO PAYLOAD VALIDATION SUMMARY',
            details: JSON.stringify({
                totalFilesChecked: totalFiles,
                passedFiles: passedFiles,
                filesMissingRefId: missingFiles,
                totalPayloadsMissingRefId: totalMissingPayloads,
                filesWithNoPayload: noPayloadFiles,
                errorFiles: errorFiles
            })
        });

        summary.mapSummary.errors.iterator().each(
            function(key, error) {

                log.error({
                    title: 'MAP ERROR - ' + key,
                    details: error
                });

                return true;
            }
        );
    }

    return {
        getInputData: getInputData,
        map: map,
        reduce: reduce,
        summarize: summarize
    };
});
