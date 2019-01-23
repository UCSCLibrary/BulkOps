
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
          jQuery("div#search-summary p#count").html(result["num_results"]);
        });
      },
      error: function(xhr, resp, text) {
        console.log(xhr, resp, text);
      }
    });
  });

});  
