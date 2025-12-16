const nodemailer = require("nodemailer");

function makeTransporter() {
  return nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 587),
    secure: String(process.env.SMTP_SECURE || "false") === "true", // true для 465
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });
}

async function sendResultEmail({ to, subject, text, filename, contentBuffer }) {
  const transporter = makeTransporter();

  // полезно на старте, чтобы видеть ошибки SMTP сразу:
  await transporter.verify();

  return transporter.sendMail({
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
