var AJAX_URL = '../sage_ajax.php';

function initSummary () {
  $('#advanced').hide();
  $('#conditions').hide();
}

function tooltipInitialize() {
  $('[data-toggle="tooltip"]').tooltip();
}

jQuery(document).ready(function($){
  $.ajaxSetup ({  
    cache: false  
  });
  var parms = '?query=line&return=json&formvar=term&pub=1';
  $('#lines').autocomplete({source:AJAX_URL+parms, minLength:3, select: function(event, ui){
    $('#line').find("option:contains('"+ui.item.value+"')").each(function(){
      if ($(this).text() == ui.item.value){
        $(this).prop("selected","selected");
      }
    });
    counter($('#line option:selected').length);
    $('#lines').val('');
    return false;
  }});

  // Option selection
  $('#line').change(function(){
    counter($('#line option:selected').length);
  });

  parms = '?query=line_lab&return=json&formvar=term';
  $('#line1').autocomplete({source:AJAX_URL+parms,
                            minLength:3,
                            select: function(event,ui) {
                              $.get(AJAX_URL,
                                    {query: 'metadata_line',
                                     line_id: ui.item.value
                                    },
                                    function (data) {
                                      populateLine(data,'line1m');
                                    });
                            }});
  $('#line2').autocomplete({source:AJAX_URL+parms,
                            minLength:3,
                            select: function(event,ui) {
                              $.get(AJAX_URL,
                                    {query: 'metadata_line',
                                     line_id: ui.item.value
                                    },
                                    function (data) {
                                      populateLine(data,'line2m');
                                    });
                            }});
  $('#agene').autocomplete({source:AJAX_URL+'?query=gene&return=json&formvar=term',
                           minLength:2,
                          });
});


function toggleClass (cls) {
  if ($('#show'+cls).is(':checked')) {
    $('.'+cls).show()
  }
  else {
    $('.'+cls).hide()
  }
}

function counter (num) {
  if (num == 0) {
    $('div.formalert').text('');
  }
  else if (num == 1) {
    $('div.formalert').text(num+' line selected');
  }
  else {
    $('div.formalert').text(num+' lines selected');
  }
}


function populateLine (data,eid) {
  var lm = '<table>';
  var obj = jQuery.parseJSON(data);
  lm = lm + "<tr><td>Line</td><td>"
       + obj.name + "</td></tr>"
       + "<tr><td>Line ID</td><td>"
       + obj.id + "</td></tr>"
       + "<tr><td>Lab</td><td>"
       + obj.lab_display_name + "</td></tr>"
       + "<tr><td>Gene</td><td>"
       + obj.gene;
  if (obj.synonyms)
    lm = lm + " (" + obj.synonyms + ")";
  lm = lm + "</td></tr>"
  if (obj.genotype)
    lm = lm + "<tr><td>Genotype</td><td>"
         + obj.genotype + "</td></tr>"
  if (obj.robot_id)
    lm = lm + "<tr><td>Robot ID</td><td>"
         + obj.robot_id + "</td></tr>"
  if (obj.flycore_id)
    lm = lm + "<tr><td>Fly Core ID</td><td>"
         + obj.flycore_id + "</td></tr>"
  lm = lm + "</table>";
  $('#'+eid).html(lm);
}


function setPrefix () {
  var p=$('#prefix option:selected').val();
  if (p)
    p = p + '_';
  $('#lineprefix').text(p);
}

function autofillLine () {
  var line = $('#aline').val();
  if (!line)
    return;
  var p=$('#prefix option:selected').val();
  if (p)
    line = p + '_' + line;
  $.ajax({type: 'POST',
          async: false,
          url: AJAX_URL,
          data: {
            query: 'autofill_line',
            line: line,
          },
          success: function(data) {
            var obj = jQuery.parseJSON(data);
            if (obj[0].error)
              alert(obj[0].error)
            else {
              $('#agenotype').val(obj[0].genotype);
              $('#flycoreid').val(obj[0].flycore_id);
              $('#robotid').val(obj[0].robot_id);
            }
          },
  });
}

function addLine () {
  $.ajax({type: 'POST',
          async: false,
          url: AJAX_URL,
          data: {
            query:       'insert_line',
            line1:       $("#line1").val(),
            line2:       $("#line2").val(),
            prefix:      $("#prefix").val(),
            line:        $("#aline").val(),
            lab:         $("#lab").val(),
            organism:    $("#organism").val(),
            genotype:    $("#agenotype").val(),
            gene:        $("#agene").val(),
            description: $("#description").val(),
            flycoreid:   $("#flycoreid").val(),
            robotid:     $("#robotid").val(),
            operator:    $("#_operator").val(),
            no_parent_lookup: 1,
          },
          success: function(data) {
            alert(data);
          },
          error: function(data) {
            alert(data);
          },
  });
}


function blankDiv (eid,div) {
  if ($('#'+eid).val() == '')
    $('#'+div).text('');
}


function flycoreToggle () {
  if ($('#flycore_known:checked').val() == 'Yes') {
    $('.fc').show();
    $('#fcbutton').show();
  }
  else {
    $('.fc').hide();
    $('#fcbutton').hide();
    $('#flycoreid').val('');
    $('#robotid').val('');
  }
}


function toggleConditions () {
  if ($('#advanced').is(':visible')) {
    $('#advanced').hide(); 
    $('#conditions').hide();
    $('#clinki').attr('src','/css/plus.gif');
    $('#clink').attr('title','Show search conditions');
  }                      
  else {                 
    $('#advanced').show(); 
    $('#conditions').show();
    $('#clinki').attr('src','/css/minus.gif');
    $('#clink').attr('title','Hide search conditions');
  }
}


function toggleSynonyms (e) {
  e = '#' + e;
  if ($(e).is(':visible')) {
    $(e).hide();
    $(e+'i').attr('src','/css/plus.gif');
    $(e+'l').attr('title','Show gene synonyms');
  }
  else {
    $(e).show();
    $(e+'i').attr('src','/css/minus.gif');
    $(e+'l').attr('title','Hide gene synonyms');
  }
}


function toggleVis(this_id) {
  if ($('#'+this_id).is(":visible")) {
    $('#i'+this_id).attr("src","/images/right_triangle.png");
  }
  else {
    $('#i'+this_id).attr("src","/images/down_triangle.png");
  }
  $('#'+this_id).toggle();
}


function toggleProjections () {
  if ($('#projections').is(':checked')) {
    $('.projection').show()
  }
  else {
    $('.projection').hide()
  }
}
