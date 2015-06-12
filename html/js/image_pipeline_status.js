$(function(){
  $('.detailarea').hide();
  $('#instructions').show();
});

function showDetails (this_id) {
  $('.detailarea').hide();
  $('#instructions').hide();
  $('#'+this_id).show();
}
