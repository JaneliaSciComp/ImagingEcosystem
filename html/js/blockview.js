$(function() {
  $("#start").datepicker({dateFormat: 'yy-mm-dd'});
  $("#stop").datepicker({dateFormat: 'yy-mm-dd'});
});

function tooltipInitialize() {
  $('[data-toggle="tooltip"]').tooltip();
}

function toggleReport () {
  $('#selector_row').hide();
  if ($('#entity').val() == 'LSMs') {
    $('#selector_row').show();
  }
}

function showDetail(id,create_date,name,annotator,microscope,dataset,slide_code,line,area,color) {
  var t = "<table class='detail'>"
          + "<tr><th>Image name</th><td>" + name + "</td></tr>"
          + "<tr><th>TMOG date</th><td>" + create_date + "</td></tr>"
          + "<tr><th>Annotator</th><td>" + annotator + "</td></tr>"
          + "<tr><th>Microscope</th><td>" + microscope + "</td></tr>"
          + "<tr><th>Data set</th><td>" + dataset + "</td></tr>"
          + "<tr><th>Slide code</th><td>" + slide_code + "</td></tr>"
          + "<tr><th>Line</th><td>" + line + "</td></tr>"
          + "<tr><th>Area</th><td>" + area + "</td></tr>"
          + "</table>";
  var response = 'No image';
  $.post('/workstation_imagery.php',
         {name: name},
         function(data) {
  var d = "<div style='float: left; border: 2px solid " + color + "'><div style='float:left'>"
          + data + "</div>"
          + "<div style='float: left;'>" + t + "</div></div><div style='clear: both;'></div>";
  $('#display').html(d);
         });
  return false;
}

function showSampleDetail(id,color) {
  $.post('/workstation_imagery.php',
         {id: id,
          entity: 'sample',
          style: 'card',
          color: color},
         function(data) {
           $('#display').html(data);
         });
  return false;
}

function noDetail() {
  $('#display').html('');
}

function navigate(url) {
  window.open(url);
  return false;
}
