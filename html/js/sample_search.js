$(function() {
  $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
    var n = e.currentTarget.href.indexOf("dataset");
    if (n != -1)
      $(".chosen-select").chosen({search_contains: true});
  })
});
