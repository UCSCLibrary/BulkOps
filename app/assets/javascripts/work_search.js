
jQuery(document).ready(function() {
  jQuery('button#add-works-search').click(function() {
    jQuery.ajax({
      url: 'search', // url where to submit the request
      type : "GET", // type of action POST || GET
      dataType : 'json', // data type
      data : jQuery("form#ajax-work-search").serialize(), // post data || get data
      success : function(result) {
        // you can see the result from the console
        // tab of the developer tools
        console.log(result);

        if(result["num_results"] > 0) {
          jQuery("div#search-summary p#count").html(result["num_results"]);
          jQuery("div#search-summary div#all-results").show();

          //Remove all previous search fields from form
          jQuery("input.prev-search-field").val("");

          //Add fields for this search to the form for adding to update
          var $collection_id = jQuery("#ajax-work-search #collection-id").val()
          jQuery("input#prev-collection-id").val($collection_id)

          var $admin_set_id = jQuery("#ajax-work-search #admin-set-id").val()
          jQuery("input#prev-admin-set-id").val($admin_set_id)

          var $workflow_state = jQuery("#ajax-work-search #workflow-state").val()
          jQuery("input#workflow-state").val($workflow_state)

          var $keyword = jQuery("#ajax-work-search #keyword").val()
          jQuery("input#keyword").val($keyword)
        }
        
        jQuery.each(result["results"],function(index,work){
          newli = jQuery("div#search-summary li#template").clone();
          newli.removeClass("hidden");
          newli.removeAttr("id");
          newli.children("input").val(work["id"]);
          newli.children("img").attr("src",work["thumbnail_path_ss"]);
          newli.children("div.work-title").html(work["title_tesim"].join(", "));
//          newli.children("div.work-description").html(work["description"].join(", "));
          jQuery("div#search-summary ul").append(newli);
          newli.show();

        });
      },
      error: function(xhr, resp, text) {
        console.log(xhr, resp, text);
      }
    });
  });

  jQuery("div#search-summary #add-all-results").click(function(){
    if( jQuery(this).is(':checked')) {
      jQuery('ul#search-sample').find('input').prop('disabled',true).css("opacity:0.5")
    }else{
      jQuery('ul#search-sample').find('input').prop('disabled',false).css("opacity:1")
    }
  });

});  
