const nodemailer = require("nodemailer");

let transporter;

function getTransporter() {
  if (transporter) return transporter;

  transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 587),
    secure: String(process.env.SMTP_SECURE || "false") === "true", // true для 465
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },

    // полезно для прод-стабильности
    pool: true,
    maxConnections: 3,
    maxMessages: 50,
    connectionTimeout: 20_000,
    greetingTimeout: 20_000,
    socketTimeout: 30_000,
  });

  // Проверка один раз (не обязательно, но удобно для логов)
  transporter.verify().then(
    () => console.log("SMTP ready"),
    (err) => console.error("SMTP verify failed:", err?.message || err)
  );

  return transporter;
}

async function sendResultEmail({ to, subject, text, filename, contentBuffer }) {
  const t = getTransporter();

  return t.sendMail({
    from: process.env.MAIL_FROM || process.env.SMTP_USER,
    to,
    subject,
    text,
    attachments: [
      {
        filename,
        content: contentBuffer, // Buffer
      },
    ],
  });
}

module.exports = { sendResultEmail };
