self.addEventListener('push', event => {
  const data = event.data?.json() || {};
  event.waitUntil(
    self.registration.showNotification(data.title || '42 Streak House', {
      body: data.body || 'Your pick result is in!',
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      data: { url: data.url || 'https://42streakhouse.com' }
    })
  );
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(clients.openWindow(event.notification.data.url));
});
