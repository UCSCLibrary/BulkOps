jQuery(document).ready(function() {

  jQuery('button.select-all').click(function() {
    jQuery(this).parent().siblings('ul').find('input').prop('checked',true)
  });
  jQuery('button.deselect-all').click(function() {
    jQuery(this).parent('div').siblings('ul').find('input').prop('checked',false)
  });

  jQuery('input.bulk-ops-index-checkbox').change(function(){
    jQuery('input#bulk-ops-select-all').prop('checked', false);
  });

  jQuery('input#bulk-ops-select-all').click(function(){
    if( jQuery(this).is(':checked')) {
      jQuery('input.bulk-ops-index-checkbox').prop('checked',true)
    } else {
      jQuery('input.bulk-ops-index-checkbox').prop('checked',false)
    }
  });

  

}
