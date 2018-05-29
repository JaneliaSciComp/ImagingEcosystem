$(function(){
  $(".chosen-select").chosen({search_contains: true});
});

function refreshCrosses() {
  var dbd_lines = {};
  $(".lineselect").each(function() {
    if ($(this).is(':checked')) {
      dbd_lines[this.id] = 1;
    }
  });
  var cross_html = '';
  for (key in dbd_lines) {
    //cross_html += $('.line').attr('id') + '-x-' + key + '<br>';
    cross_html += key + '<br>';
  }
  $('#crosses').html(cross_html);
  if (cross_html.length)
    $('#crossarea').show();
  else
    $('#crossarea').hide();
}


function hideChecked() {
  $(".lineselect").each(function() {
    if ($(this).is(':checked')) {
      cid = $($($(this).parent()).parent()).parent().attr('id');
      $('#'+cid).hide();
    }
  });
}


function hideUnchecked() {
  $(".lineselect").each(function() {
    if ($(this).is(':checked') == false) {
      cid = $($($(this).parent()).parent()).parent().attr('id');
      $('#'+cid).hide();
    }
  });
}


function showAll() {
  $(".lineselect").each(function() {
    cid = $($($(this).parent()).parent()).parent().attr('id');
    $('#'+cid).show();
  });
}
