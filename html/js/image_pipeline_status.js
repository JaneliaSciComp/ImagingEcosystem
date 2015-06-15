$(function(){
  $('.detailarea').hide();
  $('#instructions').show();
});

function showDetails (this_id) {
  $('.detailarea').hide();
  $('#instructions').hide();
  $('#'+this_id).show();
}

function toggleClass (cls) {
  if ($('#show_'+cls).is(':checked')) {
    $('.'+cls).show()
  }
  else {
    $('.'+cls).hide()
  }
}
