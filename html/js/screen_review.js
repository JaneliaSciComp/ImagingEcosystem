$(function(){
  $("#verify").prop("disabled",true);
  $("#verify").click(function(event) {
    var error_free = false;
    var total = 0;
    $.each(cross_types, function(i,l) {
      total += count[l];
    });
    if (total)
      error_free = true;
    if (!error_free){
      event.preventDefault();
    }
  });
});

function tagCross(this_id) {
  var p = $('#'+this_id+'_polarity_cross').is(':checked');
  var m = $('#'+this_id+'_mcfo_cross').is(':checked');
  var s = $('#'+this_id+'_stabilization_cross').is(':checked');
  var pid = '#'+this_id+'_polarity_cross';
  if (p || m || s) {
    $(pid).parent().parent().parent().css('background-color', '#060');
  }
  else {
    $(pid).parent().parent().parent().css('background-color', '#333');
  }
  var count = {polarity:0, mcfo:0, stabilization:0};
  var cross_types = ['polarity','mcfo','stabilization'];
  $(".line").each(function() {
    eid = $(this).attr('id');
    $.each(cross_types, function(i,l) {
      if ($('#'+eid+'_'+l+'_cross').is(':checked'))
        count[l]++;
    });
  });
  var total = 0;
  $.each(cross_types, function(i,l) {
    $('div.' + l + '_crosses').html(count[l]);
    total += count[l];
  });
  if (total)
    $("#verify").prop("disabled",false);
  else
    $("#verify").prop("disabled",true);
}

function hideChecked() {
  $(".line").each(function() {
    eid = $(this).attr('id');
    var p = $('#'+eid+'_polarity_cross').is(':checked');
    var m = $('#'+eid+'_mcfo_cross').is(':checked');
    var s = $('#'+eid+'_stabilization_cross').is(':checked');
    if (p || m || s) {
      $(this).hide();
    }
  });
}


function hideUnchecked() {
  $(".line").each(function() {
    eid = $(this).attr('id');
    var p = $('#'+eid+'_polarity_cross').is(':checked');
    var m = $('#'+eid+'_mcfo_cross').is(':checked');
    var s = $('#'+eid+'_stabilization_cross').is(':checked');
    if (!p && !m && !s) {
      $(this).hide();
    }
  });
}

function hideByClass(c) {
  $('.'+c).hide();
}

function showAll() {
  $(".line").each(function() {
    $(this).show();
  });
}
