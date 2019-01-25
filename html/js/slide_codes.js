var SAGE_RESPONDER = 'http://sage_responder.int.janelia.org';
var data_set = '';
var slide_code = '';
var objective = ''

function displayError (data,divtag) {
  if (data.responseJSON.rest.error) {
    msg = data.responseJSON.rest.error;
  }
  else {
    msg = data.responseText;
  }
  $("div#" + divtag).html('<span style="color:red">' + msg + '</span>');
}

function showDetail(id,create_date,name,annotator,microscope,dataset,slide_code,line,area,tile) {
  var t = "<table class='detail'>"
          + "<tr><th>Image name</th><td>" + name + "</td></tr>"
          + "<tr><th>TMOG date</th><td>" + create_date + "</td></tr>"
          + "<tr><th>Annotator</th><td>" + annotator + "</td></tr>"
          + "<tr><th>Microscope</th><td>" + microscope + "</td></tr>"
          + "<tr><th>Data set</th><td>" + dataset + "</td></tr>"
          + "<tr><th>Slide code</th><td>" + slide_code + "</td></tr>"
          + "<tr><th>Line</th><td>" + line + "</td></tr>"
          + "<tr><th>Area</th><td>" + area + "</td></tr>"
          + "<tr><th>Tile</th><td>" + tile + "</td></tr>"
          + "</table>";
  var response = 'No image';
  $.post('/cgi-bin/workstation_imagery_ajax.cgi',
         {name: name,
         function(data) {
  var d = "<div class='left'><div class='left'>" + data + "</div>"
          + "<div class='left'>" + t + "</div></div><div class='clear'></div>";
  $('#display').html(d);
         });
  return false;
}

function spinner(divtag,msg) {
  $("div#"+divtag).text('');
  $('<img />').attr({src: '/images/loading.gif'}).appendTo("div#"+divtag);
  $("div#"+divtag).append(' '+msg);
}

$(document).ready(function() {
  // Populate data_set block
  spinner('data_set_block','Fetching data sets');
  var sql = 'family=' + $("#family").val()
            + '&_columns=data_set&_distinct=1&_sort=data_set';
  if ($("#family").val() == 'all') {
    sql = '_columns=data_set&_distinct=1&_sort=data_set';
  }
  $.getJSON(SAGE_RESPONDER + '/image_classifications?' + sql,
      function(data) {})
      .success(function(data) {
        var s = $('<select id="data_set" name="data_set"/>');
        s.append('<option value="">Choose a data set</option>');
        $.each(data.image_classification_data, function(key, val) {
          if (key) {
            s.append($('<option/>').html(val.data_set));
          }
        });
        $("div#data_set_block").html('Data set: ');
        $("div#data_set_block").append(s)
      })
     .fail(function(data) {
       displayError(data,'data_set_block');
      });
  // Data set block
  $('body').on('change', '#data_set', function() {
    data_set = $(this).val();
    if (data_set) {
      $("div#objective_block").html('')
      $("div#data_block").html('')
      $("div#display").html('')
      spinner('slide_code_block','Fetching slide codes for ' + data_set);
      $.getJSON(SAGE_RESPONDER + '/images?_columns=slide_code&_distinct=1&data_set=' + $(this).val(),
        function(data) {})
        .success(function(data) {
          var s = $('<select id="slide_code"/>');
          s.append('<option value="">Choose a slide code</option>');
          var schash = {};
          $.each(data.image_data, function(key, val) {
            sc = val.slide_code;
            shortsc = sc.replace(/_[A-Z][0-9][0-9]*$/,'');
            if (shortsc != sc) {
              schash[shortsc] = shortsc + '*';
            }
            else {
              schash[sc] = sc;
            }
          });
          var keys = [];
          for (var key in schash) {
            keys.push(key);
          }
          keys.sort();
          for (var i in keys) {
            key = keys[i];
            s.append('<option value="' + schash[key] + '">' + key + '</option>');
          }
          $("div#slide_code_block").html('Slide code: ');
          $("div#slide_code_block").append(s)
         })
        .fail(function(data) {
          displayError(data,'slide_code_block');
         });
    }
  });

  // Slide code block
  $('body').on('change', '#slide_code', function() {
    slide_code = $(this).val()
    if (slide_code) {
      spinner('objective_block','Fetching objectives for ' + data_set + '/' + slide_code);
      $("div#data_block").html('')
      $("div#display").html('')
      $.getJSON(SAGE_RESPONDER + '/images?_columns=objective&_distinct=1&data_set=' + data_set + '&slide_code=' + slide_code,
      function(data) {})
      .success(function(data) {
        var s = $('<select id="objective"/>');
        s.append('<option value="">Choose an objective</option>');
        $.each(data.image_data, function(key, val) {
          s.append($('<option/>').html(val.objective));
        });
        $("div#objective_block").html('Objective: ');
        $("div#objective_block").append(s)
       })
      .fail(function(data) {
        displayError(data,'objective_block');
      });
    }
  });

  // Objective block
  $('body').on('change', '#objective', function() {
    objective = $(this).val()
    if (objective) {
      spinner('data_block','Fetching data for ' + data_set + '/' + slide_code + '/' + objective);
      $("div#display").html('')
      $.getJSON(SAGE_RESPONDER + '/images?_columns=id,slide_code,capture_date,create_date,microscope,microscope_filename,cross_barcode,line,name,annotated_by,area,tile&_distinct=1&data_set=' + data_set + '&slide_code=' + slide_code + '&objective=' + objective,
        function(data) {})
        .success(function(data) {
        var t = $('<table id="itable"></table>').attr({class:"tablesorter standard"});
        var thead = $('<thead></thead>').appendTo(t);
        var row = $('<tr></tr>').appendTo(thead);
        $.each(['Slide code','Capture date','TMOG date','Microscope','Microscope filename','Cross barcode','Line','Image name'], function(i,v) {
          $('<th></th>').text(v).appendTo(row);
        });
        $.each(data.image_data, function(key, val) {
          var cd = new Date(val.capture_date);
          var td = new Date(val.create_date);
          var line_link = "http://informatics-prod.int.janelia.org/cgi-bin/lineman.cgi?line=" + val.line;
          line_link = $('<a>',{text: val.line,
                        href: line_link,
                        target: "_blank",
                        });
          var ws_link = 'http://webstation.int.janelia.org/search?term=' + val.name + '&type_label=LSM+Image';
          link = $('<a>',{text: val.name,
                          href: ws_link,
                          target: "_blank",
                          mouseover: function(){
                            showDetail(val.id,cd.toDateString(),
                                       val.name,val.annotated_by,
                                       val.microscope,data_set,
                                       val.slide_code,val.line,val.area,val.tile);
                            return false;},
                          mouseout: function(){ $('#display').html(''); }
                   });
          var row = $('<tr></tr>').appendTo(t);
          $.each([val.slide_code,cd.toDateString(),td.toDateString(),val.microscope,val.microscope_filename,val.cross_barcode], function(i,v) {
            $('<td></td>').text(v).appendTo(row);
          });
          $('<td></td>').html(line_link).appendTo(row);
          $('<td></td>').html(link).appendTo(row);
        });
        $("div#data_block").html(t);
        $("#itable").tablesorter();
      })
      .fail(function(data) {
        displayError(data,'data_block');
      });
    }
  });
});
