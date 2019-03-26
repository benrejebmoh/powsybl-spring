$( window ).ready(function() {
  connect();
});

function connect() {
  var socket = new SockJS('/messages');
  stompClient = Stomp.over(socket);
  stompClient.connect({}, function (frame) {
      stompClient.subscribe('/afs/v1/node_events/mem', function (notification) {
          $('#textArea').val(notification);
       });
  });
}