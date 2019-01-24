var time = new Date().getTime();
$(document.body).bind("mousemove keypress", function(e) {
  time = new Date().getTime();
});

function refresh() {
  if(new Date().getTime() - time >= 60000) 
    window.location.reload(true);
  else 
    setTimeout(refresh, 60000);
}

function reloadSetup() {
  setTimeout(refresh, 60000);
}

function designate(image_id,line_id,image_name,user_name) {
  var status = $('#'+image_id).is(':checked');
  if (status) {
    fetch('http://sage_responder.int.janelia.org/session',{
          method: 'post',
          mode: 'cors',
          headers: { 'Access-Control-Allow-Origin': '*',
                     'Content-Type': 'application/json' },
          body: JSON.stringify({cv: 'flylight_public_annotation',
                                type: 'annotation_image',
                                name: image_name,
                                line_id: line_id,
                                image_id: image_id,
                                annotator: user_name,
                                lab: 'flylight'})
          }).then(function(response) {
            return response.json();
          }).then(function(data) {
            console.log(data.row_count);
         });
  }
  else {
    fetch('http://sage_responder.int.janelia.org/sessions?'
          + 'cv=flylight_public_annotation&type=annotation_image&lab=flylight&'
          + 'line_id=' + line_id + '&image_id=' + image_id,{
          method: 'get',
          mode: 'cors',
          headers: { 'Access-Control-Allow-Origin': '*' }
          }).then(function(response) {
            return response.json();
          }).then(function(data) {
            session_id = data.session_data[0].id;
            fetch('http://sage_responder.int.janelia.org/session/'
                  + session_id,{
                  method: 'delete',
                  mode: 'cors',
                  headers: { 'Access-Control-Allow-Origin': '*' }
                  }).then(function(response) {
                    return response.json();
                  }).then(function(data) {
                    alert('Deselected '+image_name);
                 });
         });
  }
}
