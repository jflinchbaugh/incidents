const scheduleReload = function(period, delay) {
  const now = new Date().getTime();
  const nextMinute = Math.trunc((now + period) / period) * period;
  const waitTime = nextMinute - now + delay;
  console.log(location.href);
  setTimeout(
    function() {
      var request = new XMLHttpRequest();
      request.onreadystatechange = function() {
        if (this.readyState == this.HEADERS_RECEIVED) {
          location.reload();
        }
      };
      request.open('GET', location.href, true);
      request.responseType = 'text/plain';
      request.send();
      scheduleReload(period, delay);
    },
    waitTime);
};
