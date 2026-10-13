/**
* @NApiVersion 2.1
* @NScriptType MapReduceScript
*/
define(['N/search', 'N/log', 'N/record', 'N/format', 'N/task', 'N/runtime'], function(search, log, record, format, task, runtime) {

  function getInputData() {
    // Create and return the search object

    var mapArray = [];

    try {
      log.debug('start')

    //  var firstSearch = search.create({
      return search.create({
          type: "transaction",
          filters: [
             ["mainline", "is", "T"],
             "AND",
             ["custbody_ft_update_stops", "is", "T"]
             // 'AND',
             // ['internalid', 'anyof', '15095368']

          ],
    columns: [
        search.createColumn({ name: "type", label: "Rec Type" }),
        search.createColumn({ name: "internalid", label: "Internal ID", sort: search.Sort.DESC }),
        search.createColumn({ name: "custbody_ft_related_stop_details", label: "Related Stop Details" })
    ]
});

var pagedResults = firstSearch.runPaged({ pageSize: 1000 });
pagedResults.pageRanges.forEach(function(pageRange) {
    var currentPage = pagedResults.fetch({ index: pageRange.index });
    currentPage.data.forEach(function(result) {
        var object = {
            recType: result.getValue({ name: "type" }),
            internalID: result.getValue({ name: "internalid" }),
            details: result.getValue({ name: "custbody_ft_related_stop_details" })
        };
        mapArray.push(object);
    });
});

      var secondSearch = search.create({
        type: "salesorder",
        settings:[{"name":"consolidationtype","value":"ACCTTYPE"}],
        filters:
        [
          ["type","anyof","SalesOrd"],
          "AND",
          ["custbody_sourcesystem","anyof","2"],
          "AND",
          ["custbody_ft_cancelled_po_so","is","F"],
          "AND",
          ["mainline","is","T"],
          "AND",
          ["custbody_ft_related_stop_details","isnotempty",""],
          "AND",
          ["custbody_ft_related_stop_details","isnot","[]"],
          "AND",
          ["custbody_ft_update_stops","is","F"],
          "AND",
          ["count(custrecord_ft_sales_order.internalid)","equalto","0"]
        ],
        columns:
        [
          search.createColumn({
            name: "tranid",
            summary: "GROUP",
            label: "Document Number"
          }),
          search.createColumn({
            name: "formulatext",
            summary: "GROUP",
            formula: "{custbody_ft_related_stop_details}",
            label: "Formula (Text)"
          }),
          search.createColumn({
            name: "custbody_ft_update_stops",
            summary: "GROUP",
            label: "Update Stops"
          }),
          search.createColumn({
            name: "internalid",
            summary: "GROUP",
            label: "Internal ID"
          })
        ]
      });
// var pagedResults = secondSearch.runPaged({ pageSize: 2000 });
// pagedResults.pageRanges.forEach(function(pageRange) {
//     var currentPage = pagedResults.fetch({ index: pageRange.index });
//     currentPage.data.forEach(function(result) {
//         var object = {
//           recType: 'salesorder',
//           internalID: result.getValue({name: "internalid", summary: "GROUP"}),
//           details: result.getValue({name: "formulatext", summary: "GROUP"})
//         }
//         mapArray.push(object);
//     });
// });



      // var searchResultCount = salesorderSearchObj.runPaged().count;
      // salesorderSearchObj.run().each(function(result){

      //   var object = {
      //     recType: 'salesorder',
      //     internalID: result.getValue({name: "internalid", summary: "GROUP"}),
      //     details: result.getValue({name: "formulatext", summary: "GROUP"})
      //   }

      //   mapArray.push(object);

      //   return true;
      // });

      log.debug('mapArray', mapArray.length)
      return mapArray[0];
    } catch (error) {
      log.error('error', error)
    }

  }

  function map(context) {
    // Process each search result
    var result = JSON.parse(context.value);
    log.audit('result', result)

    // Pull these out of the try so the post-processing block below can use them
    var recType = result.recordType;
    var recId = result.id;
    var scriptPayload = result.values.custbody_ft_related_stop_details || '';

    // Tracks whether anything failed while processing this record.
    // If it stays false, we clear the checkbox; otherwise we leave it checked
    // so the next run re-processes this record.
    var hasError = false;

    try {

      var details = result.values.custbody_ft_related_stop_details;
      log.audit('recId', recId)
      log.debug('recType', recType)

      if (details) {
        var object = JSON.parse(details);
      }else{
        var object = '';
      }

      log.debug('Data',{recType, recId, object})
      log.debug('object', object.length)
      var shipmentArray = [];

      if (recType == 'salesorder' || recType == 'SalesOrd') {




        for (var i = 0; i < object.length; i++) {
          log.debug('i', i)
          var shipmentID = object[i].custrecord_ft_ship_refid_1;
          shipmentArray.push(shipmentID);

          log.debug('shipmentID', shipmentID)

          var customrecord_related_stopsSearchObj = search.create({
            type: "customrecord_related_stops",
            filters:
            [
              ["custrecord_ft_sales_order","anyof", recId],
              "AND",
              ["custrecord_ft_ship_refid_1","is", shipmentID]
            ],
            columns:
            [
              search.createColumn({name: "internalid", label: "Internal ID"}),
              search.createColumn({name: "custrecord_ft_related_stops", label: "Related Stop"}),
              search.createColumn({
                name: "custrecord_ft_ship_po",
                join: "CUSTRECORD_FT_RELATED_STOPS",
                label: "Purchase Order"
              }),
              search.createColumn({
                name: "custrecord_ft_ship_refid",
                join: "CUSTRECORD_FT_RELATED_STOPS",
                label: "Stop Reference ID"
              })
            ]
          });
          var searchResultCount = customrecord_related_stopsSearchObj.runPaged().count;
          log.debug("customrecord_related_stopsSearchObj result count 196",searchResultCount);
          customrecord_related_stopsSearchObj.run().each(function(result){

            var custID = result.getValue({name: "internalid"})
            var relatedStop = result.getValue({name: "custrecord_ft_related_stops"})
            var relatedStopShipId = result.getValue({name: "custrecord_ft_ship_refid", join: "CUSTRECORD_FT_RELATED_STOPS"})

            log.debug('relatedStopShipId', relatedStopShipId)
            log.debug('relatedStop', relatedStop)
            log.debug('shipmentID', shipmentID)


            if (relatedStop) {
              if ((shipmentID == relatedStopShipId) && relatedStopShipId) {
                var shipmentRec = record.load({type:'customrecord_ft_ship_stop',id:relatedStop})

                var relatedSalesOrders = shipmentRec.getValue({fieldId:'custrecord_related_so'}) || [];
                log.debug('relatedSalesOrders', relatedSalesOrders)

                var newArray = [];

                if (relatedSalesOrders.length > 0) {
                  newArray = JSON.parse("[" +relatedSalesOrders+ "]");
                }

                if (newArray.indexOf(recId) == -1) {
                  newArray.push(recId);
                  shipmentRec.setValue({fieldId:'custrecord_related_so',value:newArray})
                  shipmentRec.save();
                }

              }else{
                log.debug('Change Stop Linkage')
                var shipmentRec = record.load({type:'customrecord_ft_ship_stop',id:relatedStop})

                var relatedSalesOrders = shipmentRec.getValue({fieldId:'custrecord_related_so'}) || [];
                log.debug('relatedSalesOrders', relatedSalesOrders)

                var newArray = [];

                if (relatedSalesOrders.length > 0) {
                  newArray = JSON.parse("[" +relatedSalesOrders+ "]");
                }

                if (newArray.indexOf(recId) != -1) {

                  var newRelatedarray = [];
                  for (var x = 0; x < newArray.length; x++) {
                    if (newArray[x] != recId) {
                      newRelatedarray.push(newArray[x]);
                    }
                  }
                  shipmentRec.setValue({fieldId:'custrecord_related_so',value:newRelatedarray})
                  shipmentRec.save();
                }

                var customrecord_ft_ship_stopSearchObj = search.create({
                  type: "customrecord_ft_ship_stop",
                  filters:
                  [
                    ["custrecord_ft_ship_refid","is", shipmentID],
                    "AND",
                    ["isinactive","is","F"]
                  ],
                  columns:
                  [
                    search.createColumn({name: "internalid", label: "Internal ID"}),
                    search.createColumn({name: "custrecord_ft_ship_po", label: "Purchase Order"}),
                    search.createColumn({name: "custrecord_related_so", label: "Related Sales Orders"})
                  ]
                });
                var searchResultCount = customrecord_ft_ship_stopSearchObj.runPaged().count;
                log.debug("customrecord_ft_ship_stopSearchObj result count",searchResultCount);
                customrecord_ft_ship_stopSearchObj.run().each(function(result){

                  var relatedStopsId = result.getValue({name: "internalid"});
                  var relatedPO = result.getValue({name: "custrecord_ft_ship_po"});
                  var prev_SO = result.getValue({name:"custrecord_related_so"}) || [];

                  var newArray = [];

                  if (prev_SO.length > 0) {
                    newArray = JSON.parse("[" +prev_SO+ "]");
                  }

                  record.submitFields({type:'customrecord_related_stops',id:custID,values: {custrecord_ft_related_stops: relatedStopsId, custrecord_ft_stop_po: relatedPO}})

                  if (newArray.indexOf(recId) == -1) {
                    newArray.push(recId);
                    record.submitFields({type:'customrecord_ft_ship_stop',id:recId,values: {custrecord_related_so: newArray}})
                  }

                  return true;
                });
              }
            }
            else{
              var customrecord_ft_ship_stopSearchObj = search.create({
                type: "customrecord_ft_ship_stop",
                filters:
                [
                  ["custrecord_ft_ship_refid","is", shipmentID],
                  "AND",
                  ["isinactive","is","F"]
                ],
                columns:
                [
                  search.createColumn({name: "internalid", label: "Internal ID"}),
                  search.createColumn({name: "custrecord_ft_ship_po", label: "Purchase Order"}),
                  search.createColumn({name: "custrecord_related_so", label: "Related Sales Orders"})
                ]
              });
              var searchResultCount = customrecord_ft_ship_stopSearchObj.runPaged().count;
              log.debug("customrecord_ft_ship_stopSearchObj result count",searchResultCount);
              customrecord_ft_ship_stopSearchObj.run().each(function(result){

                var relatedStopsId = result.getValue({name: "internalid"});
                var relatedPO = result.getValue({name: "custrecord_ft_ship_po"});
                var prev_SO = result.getValue({name:"custrecord_related_so"}) || [];

                var newArray = [];

                if (prev_SO.length > 0) {
                  newArray = JSON.parse("[" +prev_SO+ "]");
                }

                record.submitFields({type:'customrecord_related_stops',id:custID,values: {custrecord_ft_related_stops: relatedStopsId, custrecord_ft_stop_po: relatedPO}})

                if (newArray.indexOf(recId) == -1) {
                  newArray.push(recId);
                  record.submitFields({type:'customrecord_ft_ship_stop',id:relatedStopsId,values: {custrecord_related_so: newArray}})
                }

                return true;
              });
            }
            return true;
          });
          log.debug('searchResultCount', searchResultCount)
          if (searchResultCount == 0) {

            var newCreatedStop = record.create({type: 'customrecord_related_stops'})
            newCreatedStop.setValue({fieldId:'custrecord_ft_sales_order',value:recId})
            newCreatedStop.setValue({fieldId:'custrecord_ft_ship_refid_1',value:shipmentID})
            var newStopID = newCreatedStop.save();
            log.debug('newStopID', newStopID)


            var customrecord_ft_ship_stopSearchObj = search.create({
              type: "customrecord_ft_ship_stop",
              filters:
              [
                ["custrecord_ft_ship_refid","is", shipmentID],
                "AND",
                ["isinactive","is","F"]
              ],
              columns:
              [
                search.createColumn({name: "internalid", label: "Internal ID"}),
                search.createColumn({name: "custrecord_ft_ship_po", label: "Purchase Order"}),
                search.createColumn({name: "custrecord_related_so", label: "Related Sales Orders"})
              ]
            });
            var searchResultCount = customrecord_ft_ship_stopSearchObj.runPaged().count;
            log.debug("customrecord_ft_ship_stopSearchObj result count",searchResultCount);
            customrecord_ft_ship_stopSearchObj.run().each(function(result){

              var relatedStopsId = result.getValue({name: "internalid"});
              var relatedPO = result.getValue({name: "custrecord_ft_ship_po"});
              var prev_SO = result.getValue({name:"custrecord_related_so"}) || [];

              var newArray = [];

              if (prev_SO.length > 0) {
                newArray = JSON.parse("[" +prev_SO+ "]");
              }

              log.debug('relatedStopsId', relatedStopsId)


              record.submitFields({type:'customrecord_related_stops',id: newStopID,values: {custrecord_ft_related_stops: relatedStopsId, custrecord_ft_stop_po: relatedPO}})

              if (newArray.indexOf(recId) == -1) {
                newArray.push(recId);
                record.submitFields({type:'customrecord_ft_ship_stop',id:relatedStopsId,values: {custrecord_related_so: newArray}})
              }

              return true;
            });
          }

        }

        var customrecord_related_stopsSearchObj = search.create({
          type: "customrecord_related_stops",
          filters:
          [
            ["custrecord_ft_sales_order","anyof", recId]
          ],
          columns:
          [
            search.createColumn({name: "internalid", label: "Internal ID"}),
            search.createColumn({name: "custrecord_ft_ship_refid_1", label: "Shipment ID"}),
            search.createColumn({name: "custrecord_ft_related_stops", label: "Related Stop"}),
            search.createColumn({name: "custrecord_related_so", join: 'CUSTRECORD_FT_RELATED_STOPS', label: "Related Sos"})
          ]
        });
        var searchResultCount = customrecord_related_stopsSearchObj.runPaged().count;
        log.debug("customrecord_related_stopsSearchObj result count 404",searchResultCount);
        customrecord_related_stopsSearchObj.run().each(function(result){

          var systemshiprefID = result.getValue({name: "custrecord_ft_ship_refid_1"})
          var deleteRecID = result.getValue({name: "internalid"})

          if (shipmentArray.indexOf(systemshiprefID) == -1) {
            record.submitFields({type:'customrecord_related_stops',id: deleteRecID,values:{custrecord_ft_sales_order: '', custrecord_ft_related_stops: '', isinactive: true}})
            var relatedSalesOrdersID = result.getValue({name: "custrecord_related_so", join: 'CUSTRECORD_FT_RELATED_STOPS'}) || [];
             log.audit('relatedSalesOrdersID', relatedSalesOrdersID)

             var newArray = [];

             if (relatedSalesOrdersID.length > 0) {
               newArray = JSON.parse("[" +relatedSalesOrdersID+ "]");
             }

            if (newArray.indexOf(recId) != -1) {

              var newArrayObj = [];

              for (var m = 0; m < newArray.length; m++) {
                if (newArray[m] != recId) {
                  newArrayObj.push(newArray[m])
                }
              }

              record.submitFields({type:'customrecord_ft_ship_stop',id: result.getValue({name: "custrecord_ft_related_stops"}),values:{custrecord_related_so: newArrayObj}})
            }

          }
          return true;
        });

        log.debug('shipmentArray', shipmentArray)


        // NOTE: checkbox is no longer cleared here. It is cleared in the
        // post-processing block below, only when no error occurred.
        log.debug('Sales Order Processed', 'Sales Order Processed')
      }

//*******************************************************************************

      if (recType == 'purchaseorder' || recType == 'PurchOrd') {

        for (var i = 0; i < object.length; i++) {
          var shipmentID = object[i].custrecord_ft_ship_refid;


          if (!shipmentID) {
            log.debug('No shipment', 'Shipment ID is not present in Payload')
            continue;
          }
          log.debug('shipmentID', shipmentID)

          shipmentArray.push(shipmentID);


          if (object[i].custrecord_ft_ship_plandate && object[i].custrecord_ft_ship_plandate.indexOf(" ") !== -1) {
              object[i].custrecord_ft_ship_plandate = new Date(object[i].custrecord_ft_ship_plandate.split(" ")[0]);
          }else if (object[i].custrecord_ft_ship_plandate){
            object[i].custrecord_ft_ship_plandate = new Date(object[i].custrecord_ft_ship_plandate);
          }
          if (object[i].custrecord_ft_ship_actualdate && object[i].custrecord_ft_ship_actualdate.indexOf(" ") !== -1) {
              object[i].custrecord_ft_ship_actualdate = new Date(object[i].custrecord_ft_ship_actualdate.split(" ")[0]);
          }else if (object[i].custrecord_ft_ship_actualdate){
            object[i].custrecord_ft_ship_actualdate = new Date(object[i].custrecord_ft_ship_actualdate);
          }
          if (object[i].custrecord_ft_ship_appt && object[i].custrecord_ft_ship_appt.indexOf(" ") !== -1) {
              object[i].custrecord_ft_ship_appt = new Date(object[i].custrecord_ft_ship_appt.split(" ")[0]);
          }else if (object[i].custrecord_ft_ship_appt){
            object[i].custrecord_ft_ship_appt = new Date(object[i].custrecord_ft_ship_appt);
          }


          if (object[i].custrecord_ft_shipstop_apptime && object[i].custrecord_ft_shipstop_apptime.indexOf(":") !== -1){
            object[i].custrecord_ft_shipstop_apptime = convertTo12HourFormat(object[i].custrecord_ft_shipstop_apptime);
          }
          if (object[i].custrecord_ft_stop_plantime && object[i].custrecord_ft_stop_plantime.indexOf(":") !== -1){
            object[i].custrecord_ft_stop_plantime = convertTo12HourFormat(object[i].custrecord_ft_stop_plantime);
          }
          if (object[i].custrecord_ft_stop_actualtime && object[i].custrecord_ft_stop_actualtime.indexOf(":") !== -1){
            object[i].custrecord_ft_stop_actualtime = convertTo12HourFormat(object[i].custrecord_ft_stop_actualtime);
          }

          log.debug('object', object[i])

          log.audit('search filter', {recId, shipmentID})
          var shipmentrecID = '';
          var customrecord_ft_ship_stopSearchObj = search.create({
            type: "customrecord_ft_ship_stop",
            filters:
            [
              ["custrecord_ft_ship_po","anyof",recId],
              "AND",
              ["custrecord_ft_ship_refid","is",shipmentID],
              "AND",
              ["isinactive","is","F"]
            ],
            columns:
            [
              search.createColumn({name: "internalid", label: "Internal ID"}),
              search.createColumn({name: "custrecord_related_so", label: "Related Sales Orders"})
            ]
          });
          var searchResultCount = customrecord_ft_ship_stopSearchObj.runPaged().count;
          log.debug("customrecord_ft_ship_stopSearchObj result count",searchResultCount);
          customrecord_ft_ship_stopSearchObj.run().each(function(result){

            shipmentrecID = result.getValue({name:'internalid'});
            record.submitFields({type:'customrecord_ft_ship_stop',id:shipmentrecID,values:object[i]});


            return true;
          });

          if (searchResultCount == 0) {
            var newRec = record.create({type: 'customrecord_ft_ship_stop'})

            newRec.setValue({
              fieldId: 'custrecord_ft_ship_po',
              value: recId
            });

            for (var fieldId in object[i]) {
              if (object[i].hasOwnProperty(fieldId)) {

                if (fieldId == "custrecord_ft_shipstop_apptime" || fieldId == "custrecord_ft_stop_plantime" || fieldId == "custrecord_ft_stop_actualtime") {
                  newRec.setText({
                  fieldId: fieldId,
                  text: object[i][fieldId]
                });
                }else{
                newRec.setValue({
                  fieldId: fieldId,
                  value: object[i][fieldId]
                });
                }
              }
            }

            shipmentrecID = newRec.save();
            log.debug('shipmentrecID', shipmentrecID)
          }


          var relatedSalesorders = [];

          var customrecord_related_stopsSearchObj = search.create({
            type: "customrecord_related_stops",
            filters:
            [
              ["custrecord_ft_ship_refid_1","is",shipmentID]
            ],
            columns:
            [
              search.createColumn({name: "internalid", label: "Internal Id"}),
              search.createColumn({name: "custrecord_ft_sales_order", label: "Sales Order"}),
              search.createColumn({name: "custrecord_ft_related_stops", label: "Related Stop"})
            ]
          });
          var searchResultCount = customrecord_related_stopsSearchObj.runPaged().count;
          log.debug("customrecord_related_stopsSearchObj result count 561",searchResultCount);
          customrecord_related_stopsSearchObj.run().each(function(result){

            var relatedStopID = result.getValue({name: "internalid"});
            var mainStopId = result.getValue({name: "custrecord_ft_related_stops"});
            var salesorderID = result.getValue({name: "custrecord_ft_sales_order"})

            relatedSalesorders.push(salesorderID);


            if ((mainStopId != shipmentrecID) && mainStopId) {

              var relatedstoRec = record.load({type:'customrecord_ft_ship_stop',id:mainStopId})
              var relatedSales = relatedstoRec.getValue({fieldId:'custrecord_related_so'}) || [];

      var newArray = [];

      if (relatedSales.length > 0) {
        newArray = JSON.parse("[" +relatedSales+ "]");
      }


              if (newArray.indexOf(salesorderID) != -1) {

                var newArray1 = [];
                for (var x = 0; x < newArray.length; x++) {

                  if (newArray[x] != salesorderID ) {
                    newArray1.push(newArray[x]);
                  }

                }
                record.submitFields({type:'customrecord_ft_ship_stop',id:mainStopId,values:{custrecord_related_so: removeEmptyElements(newArray1)}})
              }

            }
            record.submitFields({type:'customrecord_related_stops',id:relatedStopID,values:{custrecord_ft_related_stops: shipmentrecID, custrecord_ft_stop_po: recId}})


            return true;
          });

          log.debug('relatedSalesorders', relatedSalesorders)

          record.submitFields({type:'customrecord_ft_ship_stop',id:shipmentrecID,values:{custrecord_related_so: removeEmptyElements(relatedSalesorders)}})




        }

        log.debug('shipmentArray', shipmentArray)
        try {
                  var customrecord_related_stopsSearchObj = search.create({
          type: "customrecord_ft_ship_stop",
          filters:
          [
            ["custrecord_ft_ship_po","anyof", recId],
                    "AND",
                    ["isinactive","is","F"]
          ],
          columns:
          [
            search.createColumn({name: "internalid", label: "Internal ID"}),
            search.createColumn({name: "custrecord_ft_ship_refid", label: "Shipment ID"}),
            search.createColumn({name: "custrecord_related_so", label: "Related Sales"})
          ]
        });
        var searchResultCount = customrecord_related_stopsSearchObj.runPaged().count;
        log.debug("customrecord_related_stopsSearchObj result count 630",searchResultCount);
        customrecord_related_stopsSearchObj.run().each(function(result){

          var systemshiprefID = result.getValue({name: "custrecord_ft_ship_refid"})
          log.debug('systemshiprefID', systemshiprefID)
          var deleteRecID = result.getValue({name: "internalid"})

          if (shipmentArray.indexOf(systemshiprefID) == -1) {
            record.submitFields({type:'customrecord_ft_ship_stop',id: deleteRecID,values:{custrecord_related_so: '', custrecord_ft_ship_po: '', isinactive: true}})

            var customrecord_related_stopsSearchObj = search.create({
              type: "customrecord_related_stops",
              filters:
              [
                ["custrecord_ft_ship_refid_1","is",systemshiprefID],
                    "AND",
                    ["isinactive","is","F"]
              ],
              columns:
              [
                search.createColumn({name: "internalid", label: "Internal ID"})
              ]
            });
            var searchResultCount = customrecord_related_stopsSearchObj.runPaged().count;
            log.debug("customrecord_related_stopsSearchObj result count 652",searchResultCount);
            customrecord_related_stopsSearchObj.run().each(function(result){
              record.submitFields({type:'customrecord_related_stops',id: result.getValue({name: "internalid"}),values:{custrecord_ft_sales_order: '', custrecord_ft_related_stops: '', isinactive: true}})
              return true;
            });


          }
          return true;
        });
        } catch (error) {
          // An error in this cleanup block must also block the checkbox clear,
          // so flag it and re-throw to the outer catch.
          log.error('Final Error', error)
          throw error;
        }
        log.debug('Final PO Processed', recId)
        // NOTE: checkbox is no longer cleared here. See post-processing block below.
      }

    } catch (e) {
      // Any failure anywhere above (including RCRD_HAS_BEEN_CHANGED from a
      // shipment record being updated by another concurrent Sales Order)
      // lands here. We mark hasError so the checkbox is left checked.
      hasError = true;
      log.error('error', e)
    }

    if (!hasError) {
  try {
    var currentRecordType = (recType == 'salesorder' || recType == 'SalesOrd')
      ? 'salesorder'
      : 'purchaseorder';

    if (currentRecordType) {
      var currentRecord = record.load({
        type: currentRecordType,
        id: recId
      });

      var currentPayload = currentRecord.getValue({
        fieldId: 'custbody_ft_related_stop_details'
      }) || '';


      log.debug('scriptPayload', scriptPayload)
      log.debug('currentPayload', currentPayload)

      if (scriptPayload != currentPayload) {
        log.audit('Checkbox left CHECKED due to payload change', {
          recId: recId,
          recType: recType
        });
        return;
      }

      record.submitFields({
        type: currentRecordType,
        id: recId,
        values: {custbody_ft_update_stops: false}
      })
      
    }
  } catch (payloadCheckErr) {
    log.error('Error checking current payload for ' + recId, payloadCheckErr);
    return;
  }
    }
    else {
      log.audit('Checkbox left CHECKED due to error', recId);
    }

  }

  function reduce(context) {
  }

  function summarize(summary) {

    // // ------------------------------------------------------------------
    // // Re-schedule the script if any records still need processing.
    // // Records that errored out kept their checkbox checked, so this search
    // // will find them. When nothing is left, the count is 0 and we stop -
    // // this is what prevents an endless loop of empty runs.
    // // ------------------------------------------------------------------
    // var remaining = 0;
    // try {
    //   var pendingSearch = search.create({
    //     type: "transaction",
    //     filters: [
    //       ["mainline", "is", "T"],
    //       "AND",
    //       ["custbody_ft_update_stops", "is", "T"]
    //     ],
    //     columns: [search.createColumn({name: "internalid"})]
    //   });
    //   remaining = pendingSearch.runPaged({pageSize: 1000}).count;
    // } catch (e) {
    //   log.error('Error counting remaining records', e);
    // }
    // log.audit('Remaining records still flagged', remaining);

    // if (remaining > 0) {
    //   try {
    //     var scriptObj = runtime.getCurrentScript();
    //     var mrTask = task.create({
    //       taskType: task.TaskType.MAP_REDUCE,
    //       scriptId: scriptObj.id
    //     });
    //     var taskId = mrTask.submit();
    //     log.audit('Re-scheduled Map/Reduce', 'New task ID: ' + taskId);
    //   } catch (e) {
    //     // This commonly throws if no free deployment is available.
    //     log.error('Could not re-schedule Map/Reduce', e);
    //   }
    // } else {
    //   log.audit('No remaining records', 'Script will not re-schedule.');
    // }
  }

  function convertTo12HourFormat(time24) {
    // Split the input time string into hours and minutes
    var timeParts = time24.split(":");
    var hours = parseInt(timeParts[0], 10);
    var minutes = parseInt(timeParts[1], 10);

    // Determine AM or PM
    var period = hours >= 12 ? "pm" : "am";

    // Convert hours to 12-hour format
    hours = hours % 12 || 12; // Adjust 0 to 12 for midnight
    var formattedTime = (hours) + ":" +
                        (minutes < 10 ? "0" + minutes : minutes) + " " + period;

    // Format the time in 12-hour format

   return formattedTime;
   // return hours + ":" + minutes + " " + period;
}

  function removeEmptyElements(array) {
      var result = [];
      for (var i = 0; i < array.length; i++) {
          if (array[i] !== null && array[i] !== "") {
              result.push(array[i]);
          }
      }
      return result;
  }

  return {
    getInputData: getInputData,
    map: map,
    reduce: reduce,
    summarize: summarize
  };
});