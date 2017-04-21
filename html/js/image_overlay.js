function initialize() {
  var style = 'opacity(1)';
  $('#image1').attr('style','filter: '+style+'; -webkit-filter: '+style+';');
  $('#image1Opacity').val(100);
  $('#image1Slider').val(100);
  style = 'opacity(.7)';
  $('#image2').attr('style','filter: '+style+'; -webkit-filter: '+style+';');
  $('#image2Opacity').val(70);
  $('#image2Slider').val(70);
}

function changeSlider (e) {
  $('#'+e+'Opacity').val($('#'+e+'Slider').val());
  var style = 'opacity(' + $('#'+e+'Slider').val()/100 + ')';
  $('#'+e).attr('style','filter: '+style+'; -webkit-filter: '+style+';');
}

function changeBox (e) {
  $('#'+e+'Slider').val($('#'+e+'Opacity').val());
  var style = 'opacity(' + $('#'+e+'Opacity').val()/100 + ')';
  $('#'+e).attr('style','filter: '+style+'; -webkit-filter: '+style+';');
}
