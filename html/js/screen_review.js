var orginalHeight = 150;

function changeSlider (e) {
  var fraction = (1 + $('#'+e+'Slider').val() / 100),
  newHeight = orginalHeight * fraction;
  $("#"+e).text(Math.floor(fraction * 100) + '%');
  $('.ti').height(newHeight);
}

$(function() {
  $("#start").datepicker({dateFormat: 'yy-mm-dd'});
  $("#stop").datepicker({dateFormat: 'yy-mm-dd'});
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
  $('.edit').editable('/sage_ajax.php', {
    submitdata : {query: 'annotate_line',
                  line: 'line',
                  cv: 'line',
                  cvterm: 'screen_review_comment',
                  userid: $('#userid').val()},
    type       : 'textarea',
    indicator  : 'Saving...',
    tooltip    : 'Click to edit...',
    cancel     : 'Cancel',
    submit     : 'OK',
    style      : 'color: black',
  });
if ($("#imgt1")) {
  orginalHeight = $("#imgt1").height();
}
});

function tagCross(this_id) {
  var p = $('#'+this_id+'_polarity_cross').is(':checked');
  var m = $('#'+this_id+'_mcfo_cross').is(':checked');
  var s = $('#'+this_id+'_stabilization_cross').is(':checked');
  var d = $('#'+this_id+'_discard').is(':checked');
  var pid = '#'+this_id+'_polarity_cross';
  if (p || m || s || d) {
    $(pid).parent().parent().parent().css('background-color', '#060');
  }
  else {
    $(pid).parent().parent().parent().css('background-color', '#333');
  }
  var count = {polarity:0, mcfo:0, stabilization:0, discard:0};
  var cross_types = ['polarity','mcfo','stabilization'];
  $(".line").each(function() {
    eid = $(this).attr('id');
    $.each(cross_types, function(i,l) {
      if ($('#'+eid+'_'+l+'_cross').is(':checked'))
        count[l]++;
    });
    if ($('#'+eid+'_discard').is(':checked'))
      count['discard']++;
  });
  var total = 0;
  $.each(cross_types, function(i,l) {
    $('div.' + l + '_crosses').html(count[l]);
    total += count[l];
  });
  $('div.discards').html(count['discard']);
  total += count['discard'];
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
    var d = $('#'+eid+'_discard').is(':checked');
    if (p || m || s || d) {
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
    var d = $('#'+eid+'_discard').is(':checked');
    if (!p && !m && !s && !d) {
      $(this).hide();
    }
  });
}

function hideByClass(c) {
  $('.'+c).hide();
}

function showByClass(c) {
  $('.'+c).show();
}

function showAll() {
  $(".line").each(function() {
    $(this).show();
  });
}
