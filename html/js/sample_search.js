$(function() {
  $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
    var n = e.currentTarget.href.indexOf("dataset");
    if (n != -1)
      $(".chosen-select").chosen({search_contains: true});
  })
});

function toggleVis(this_id) {
  if ($('#'+this_id).is(":visible")) {
    $('#i'+this_id).attr("src","/images/right_triangle_small.png");
  }
  else {
    $('#i'+this_id).attr("src","/images/down_triangle_small.png");
  }
  $('#'+this_id).toggle();
}
