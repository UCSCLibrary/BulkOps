jQuery(document).ready(function() {

  jQuery('button.select-all').click(function() {
    jQuery(this).parent().siblings('ul').find('input').prop('checked',true)
  });
  jQuery('button.deselect-all').click(function() {
    jQuery(this).parent('div').siblings('ul').find('input').prop('checked',false)
  });
});
