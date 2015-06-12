$(function() {
  $("#line_id").chosen({search_contains: true});
  $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
    $(".chosen-select").chosen({search_contains: true});
  })
});
