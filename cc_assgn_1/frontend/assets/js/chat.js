var checkout = {};

$(document).ready(function () {
  var $messages = $('.messages-content'),
    d, h, m,
    i = 0;

  // Generate or retrieve sessionId
  function getSessionId() {
    let sessionId = sessionStorage.getItem("sessionId");
    if (!sessionId) {
      sessionId = "sess_" + Math.random().toString(36).substr(2, 9); // Generate random session ID
      sessionStorage.setItem("sessionId", sessionId);
    }
    return sessionId;
  }

  $(window).on("load", function () {
    $messages.mCustomScrollbar();
    insertResponseMessage("Hi there, I'm your personal Concierge. How can I help?");
  });

  function updateScrollbar() {
    setTimeout(() => {
      let chatContainer = document.querySelector(".messages");
      if (chatContainer) {
        chatContainer.scrollTop = chatContainer.scrollHeight;
        console.log("✅ Forced scroll to bottom:", chatContainer.scrollHeight);
      }
    }, 100); // Delay ensures height updates before scrolling
  }

  function setDate() {
    d = new Date();
    if (m != d.getMinutes()) {
      m = d.getMinutes();
      $('<div class="timestamp">' + d.getHours() + ':' + m + '</div>').appendTo($('.message:last'));
    }
  }

  function callChatbotApi(message) {
    return sdk.chatbotPost({}, {
      sessionId: getSessionId(),  // ✅ Include sessionId in the request
      messages: [{
        type: 'unstructured',
        unstructured: {
          text: message
        }
      }]
    }, {});
  }

  function insertMessage() {
    msg = $('.message-input').val();
    if ($.trim(msg) == '') {
      return false;
    }

    // Append user message
    $('<div class="message message-personal">' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
    setDate();
    $('.message-input').val(null);

    // Append loading bubble before scrolling
    let loadingMessage = $('<div class="message loading new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure><span></span></div>');
    loadingMessage.appendTo($('.mCSB_container'));

    updateScrollbar(); // Scroll after loading bubble is added

    callChatbotApi(msg)
      .then((response) => {
        console.log(response);
        var data = response.data;

        loadingMessage.remove(); // Remove loading bubble after response

        if (data.messages && data.messages.length > 0) {
          console.log('Received ' + data.messages.length + ' messages');
          data.messages.forEach(message => {
            if (message.type === 'unstructured') {
              insertResponseMessage(message.unstructured.text);
            }
          });
        } else {
          insertResponseMessage('Oops, something went wrong. Please try again.');
        }
      })
      .catch((error) => {
        console.log('An error occurred', error);
        loadingMessage.remove();
        insertResponseMessage('Oops, something went wrong. Please try again.');
      });
  }

  $('.message-submit').click(function () {
    insertMessage();
  });

  $(window).on('keydown', function (e) {
    if (e.which == 13) {
      insertMessage();
      return false;
    }
  });

  function insertResponseMessage(content) {
    setTimeout(function () {
      $('.message.loading').remove();
      $('<div class="message new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure>' + content + '</div>').appendTo($('.mCSB_container')).addClass('new');
      setDate();

      setTimeout(() => updateScrollbar(), 50); // Ensure height updates before scrolling
    }, 500);
  }
});
