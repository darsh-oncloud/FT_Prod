/**
 * @NApiVersion 2.x
 * @NScriptType UserEventScript
 * @NModuleScope SameAccount
 */
define(['N/record', 'N/log', 'N/search'], function(record, log, search) {

    function afterSubmit(context) {
        var newRecord = context.newRecord;
        var oldRecord = context.oldRecord;
        var recId = newRecord.id;
        var recType = newRecord.type;


        // if (recId != 9135041 && recType == 'salesorder') return;
        // if (recId != 9154173 && recType == 'purchaseorder') return;

        var related_Sodetails = newRecord.getValue({fieldId: 'custbody_ft_so_details_object'});
        var MG_Status = newRecord.getValue({fieldId: 'custbody_ft_mhpostatus'});
      

      log.debug('Values', {
        recId: recId,
        related_Sodetails: JSON.stringify(related_Sodetails),
        MG_Status: MG_Status,
        recType: recType
      })

      if (related_Sodetails && MG_Status != 6  && recType == 'purchaseorder') {
            related_Sodetails = JSON.parse(related_Sodetails)
            var SOs = [];
            for (x = 0; x < related_Sodetails.length; x++) {
              var soID = get_so_id(related_Sodetails[x].SOInternalID);   
              if (soID.returnid) SOs.push(soID.returnid)
            }
            log.debug('SOs', SOs)
            record.submitFields({type:'purchaseorder',id:recId,values:{ custbody_ds_related_trans: SOs},options:{ignoreMandatoryFields:true}})
          }

        // Determine if it's a create or edit
        var isCreate = context.type === context.UserEventType.CREATE;
        var isEdit = context.type === context.UserEventType.EDIT;

        // Handle Create event
        if (isCreate) {
            var newStopDetails = newRecord.getValue('custbody_ft_related_stop_details');

            // If 'custbody_ft_related_stop_details' has a value, set 'custbody_ft_update_stops' to true
            if (newStopDetails) {
                try {

                  record.submitFields({type:recType,id:recId,values:{custbody_ft_update_stops: true}})

                    log.debug({
                        title: 'Field Updated on Create',
                        details: 'Field custbody_ft_related_stop_details has a value. custbody_ft_update_stops set to true.'
                    });
                } catch (e) {
                    log.error({
                        title: 'Error Updating Field on Create',
                        details: e.toString()
                    });
                }
            }
        }

        // Handle Edit event
        if (isEdit) {
            var oldStopDetails = oldRecord.getValue('custbody_ft_related_stop_details');
            var newStopDetails = newRecord.getValue('custbody_ft_related_stop_details');

          log.debug('newStopDetails', newStopDetails)

            // If the values have changed, set 'custbody_ft_update_stops' to true
            if (oldStopDetails != newStopDetails) {
                try {

                    record.submitFields({type:recType,id:recId,values:{custbody_ft_update_stops: true}})

                    log.debug({
                        title: 'Field Updated on Edit',
                        details: 'Field custbody_ft_related_stop_details has changed. custbody_ft_update_stops set to true.'
                    });
                } catch (e) {
                    log.error({
                        title: 'Error Updating Field on Edit',
                        details: e.toString()
                    });
                }
            }
        }
    }

  function get_so_id(so_externalid){
    var returnid = '';
    
    
    if (so_externalid) {
      var salesorderSearchObj = search.create({
        type: "salesorder",
        filters:
        [
          ["mainline","is","T"], 
          "AND", 
          ["externalid","anyof",so_externalid]
        ],
        columns:
        [
          search.createColumn({name: "internalid", label: "Internal ID"})
          
        ]
      });
      var searchResultCount = salesorderSearchObj.runPaged().count;
      salesorderSearchObj.run().each(function(result){
        
        returnid = result.getValue({
          name: "internalid", 
          label: "Internal ID"
        })

        return true;
      })
      
    }
    return {returnid:returnid};
  }

    return {
        afterSubmit: afterSubmit
    };

});
