// Makes Hashira VRaid installable as a desktop/phone app. It does not cache anything,
// so every update you deploy shows up right away (the app always loads from the server).
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {});
