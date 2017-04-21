function changeSlider (e) {
  $('#'+e).val($('#'+e+'Slider').val());
  var style = 'brightness(' + $('#brightness').val()/100 + ') '
              + 'contrast(' + $('#contrast').val()/100 + ') '
              + 'saturate(' + $('#saturate').val()/100 + ') '
              + 'grayscale(' + $('#grayscale').val()/100 + ') '
              + 'hue-rotate(' + $('#hue-rotate').val() + 'deg) ';
  $('#img').attr('style','filter: '+style+'; -webkit-filter: '+style+';');
}

function changeBox (e) {
  $('#'+e+'Slider').val($('#'+e).val());
  var style = 'brightness(' + $('#brightness').val()/100 + ') '
              + 'contrast(' + $('#contrast').val()/100 + ') '
              + 'saturate(' + $('#saturate').val()/100 + ') '
              + 'grayscale(' + $('#grayscale').val()/100 + ') '
              + 'hue-rotate(' + $('#hue-rotate').val() + 'deg) ';
  $('#img').attr('style','filter: '+style+'; -webkit-filter: '+style+';');
}
