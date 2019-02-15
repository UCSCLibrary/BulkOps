jQuery(document).ready(function() {

  jQuery('button.select-all').click(function() {
    jQuery(this).parent().siblings('ul').find('input').prop('checked',true)
  });
  jQuery('button.select-none').click(function() {
    jQuery(this).parent('div').siblings('ul').find('input').prop('checked',false)
  });

  jQuery('div#search-summary button.select-all').click(function() {
    jQuery('ul#search-sample').find('input').prop('checked',true)
  });
  jQuery('div#search-summary button.select-none').click(function() {
    jQuery('ul#search-sample').find('input').prop('checked',false)
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

  jQuery('input#use-default-fields').click(function(){
    if( jQuery(this).is(':checked')) {
      jQuery('div#choose-fields').hide()
    }else{
      jQuery('div#choose-fields').show()
    }
  });
});
