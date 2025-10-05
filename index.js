// server.js
const express = require('express');
const path = require('path');
const app = express();
const PORT = 3000;

app.use(express.static(path.join(__dirname, 'public')));

app.get('/sse-progress', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  let progress = 0;

  const interval = setInterval(() => {
    progress += 10;

    let message = '';
    switch (progress) {
      case 10:
        message = 'Dekho, abhi 10% complete hua hai.';
        break;
      case 20:
        message = 'Abhi 20% chal raha hai... dekh rahe ho na?';
        break;
      case 30:
        message = 'Dekha? Dheere dheere badh raha hai... abhi 30% ho gaya.';
        break;
      case 40:
        message = 'Abhi 40% complete ho gaya, aise hi kaam karta hai bhai.';
        break;
      case 50:
        message = 'Half century! 50% complete ho chuka hai.';
        break;
      case 60:
        message = '60% tak aa gaye hain, final stage pass ho raha hai.';
        break;
      case 70:
        message = 'Bas 70% tak ho gaya, almost done bhai.';
        break;
      case 80:
        message = '80% tak pohch gaye, ready ho jao finishing ke liye.';
        break;
      case 90:
        message = '90% ho chuka hai... last touch bacha hai!';
        break;
      case 100:
        message = 'Done! 100% complete ho gaya. File ready hai.';
        break;
      default:
        message = `Progress: ${progress}%`;
    }

    res.write(`data: ${JSON.stringify({ progress, message })}\n\n`);

    if (progress >= 100) {
      clearInterval(interval);
      setTimeout(() => {
        res.write(`data: ${JSON.stringify({ progress: 100, complete: true })}\n\n`);
        res.end();
      }, 1000);
    }
  }, 2000);
});

app.listen(PORT, () => {
  console.log(`✅ Server running at http://localhost:${PORT}`);
});