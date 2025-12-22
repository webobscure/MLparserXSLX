"use strict";

const crypto = require("crypto");
const { google } = require("googleapis");

let oAuth2Client;
let gmail;

/** RFC 2047 для UTF-8 в Subject/From name (чтобы кириллица не ломалась) */
function encodeHeaderValue(value) {
  const s = String(value ?? "");
  // если только ASCII — оставляем как есть
  if (/^[\x00-\x7F]*$/.test(s)) return s;
  const b64 = Buffer.from(s, "utf8").toString("base64");
  return `=?UTF-8?B?${b64}?=`;
}

function base64UrlEncode(buf) {
  return Buffer.from(buf)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function getGmailClient() {
  if (gmail) return gmail;

  const {
    GMAIL_CLIENT_ID,
    GMAIL_CLIENT_SECRET,
    GMAIL_REDIRECT_URI,
    GMAIL_REFRESH_TOKEN,
  } = process.env;

  if (!GMAIL_CLIENT_ID) throw new Error("GMAIL_CLIENT_ID is not set");
  if (!GMAIL_CLIENT_SECRET) throw new Error("GMAIL_CLIENT_SECRET is not set");
  if (!GMAIL_REDIRECT_URI) throw new Error("GMAIL_REDIRECT_URI is not set");
  if (!GMAIL_REFRESH_TOKEN) throw new Error("GMAIL_REFRESH_TOKEN is not set");

  oAuth2Client = new google.auth.OAuth2(
    GMAIL_CLIENT_ID,
    GMAIL_CLIENT_SECRET,
    GMAIL_REDIRECT_URI
  );

  oAuth2Client.setCredentials({ refresh_token: GMAIL_REFRESH_TOKEN });

  gmail = google.gmail({ version: "v1", auth: oAuth2Client });
  return gmail;
}

function buildRawEmail({
  fromEmail,
  fromName,
  to,
  subject,
  text,
  html,
  attachments = [],
}) {
  const boundaryMixed = `mixed_${crypto.randomBytes(12).toString("hex")}`;
  const boundaryAlt = `alt_${crypto.randomBytes(12).toString("hex")}`;

  const headers = [
    `From: ${fromName ? `"${encodeHeaderValue(fromName)}" ` : ""}<${fromEmail}>`,
    `To: <${to}>`,
    `Subject: ${encodeHeaderValue(subject)}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/mixed; boundary="${boundaryMixed}"`,
    `Date: ${new Date().toUTCString()}`,
    `Message-ID: <${Date.now()}.${crypto.randomBytes(8).toString("hex")}@${fromEmail.split("@")[1] || "local"}>`,
    "",
  ].join("\r\n");

  // multipart/alternative part (text + html)
  const altParts = [];

  if (text) {
    altParts.push(
      [
        `--${boundaryAlt}`,
        `Content-Type: text/plain; charset="UTF-8"`,
        `Content-Transfer-Encoding: 7bit`,
        "",
        String(text),
        "",
      ].join("\r\n")
    );
  }

  if (html) {
    altParts.push(
      [
        `--${boundaryAlt}`,
        `Content-Type: text/html; charset="UTF-8"`,
        `Content-Transfer-Encoding: 7bit`,
        "",
        String(html),
        "",
      ].join("\r\n")
    );
  }

  // если html не передали — хотя бы пустой alt закрываем корректно
  if (!altParts.length) {
    altParts.push(
      [
        `--${boundaryAlt}`,
        `Content-Type: text/plain; charset="UTF-8"`,
        `Content-Transfer-Encoding: 7bit`,
        "",
        "",
        "",
      ].join("\r\n")
    );
  }

  const alternativeBlock = [
    `--${boundaryMixed}`,
    `Content-Type: multipart/alternative; boundary="${boundaryAlt}"`,
    "",
    ...altParts,
    `--${boundaryAlt}--`,
    "",
  ].join("\r\n");

  // attachments
  const attachBlocks = attachments.map((att) => {
    const filename = att.filename || "file.bin";
    const contentType = att.contentType || "application/octet-stream";
    const contentBase64 = Buffer.isBuffer(att.content)
      ? att.content.toString("base64")
      : Buffer.from(att.content).toString("base64");

    return [
      `--${boundaryMixed}`,
      `Content-Type: ${contentType}; name="${encodeHeaderValue(filename)}"`,
      `Content-Disposition: attachment; filename="${encodeHeaderValue(filename)}"`,
      `Content-Transfer-Encoding: base64`,
      "",
      // base64 желательно переносить строками по 76 символов
      contentBase64.replace(/(.{76})/g, "$1\r\n"),
      "",
    ].join("\r\n");
  });

  const end = `--${boundaryMixed}--\r\n`;

  return headers + alternativeBlock + attachBlocks.join("") + end;
}

async function sendResultEmail({ to, subject, text, filename, contentBuffer, html }) {
  const gmail = getGmailClient();

  const fromEmail = process.env.GMAIL_EMAIL;
  if (!fromEmail) throw new Error("GMAIL_EMAIL is not set");

  const raw = buildRawEmail({
    fromEmail,
    fromName: process.env.GMAIL_FROM_NAME || "ML Parser",
    to,
    subject,
    text,
    html,
    attachments: [
      {
        filename,
        contentType:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        content: contentBuffer, // Buffer
      },
    ],
  });

  const encodedMessage = base64UrlEncode(raw);

  const resp = await gmail.users.messages.send({
    userId: "me",
    requestBody: { raw: encodedMessage },
  });

  return resp.data; // { id, threadId, labelIds ... }
}

module.exports = { sendResultEmail };
