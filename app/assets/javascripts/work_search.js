
jQuery(document).ready(function() {
  jQuery('button#add-works-search').click(function() {
    jQuery.ajax({
      url: 'search', // url where to submit the request
      type : "GET", // type of action POST || GET
      dataType : 'json', // data type
      data : jQuery("form#add-works-to-draft").serialize(), // post data || get data
      success : function(result) {
        // you can see the result from the console
        // tab of the developer tools
        console.log(result);

        if(result["num_results"] > 0) {
          jQuery("div#search-summary p#count").html(result["num_results"]);
          jQuery("div#search-summary div#all-results").show();
          
          jQuery("div#search-summary div#previous-search-fields").remove();
          jQuery("div#ajax-work-search").clone().attr("id","previous-search-fields").hide().appendTo("div#search-summary > form");
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
