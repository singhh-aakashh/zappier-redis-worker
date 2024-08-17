"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.sendGmail = void 0;
const nodemailer_1 = __importDefault(require("nodemailer"));
const transporter = nodemailer_1.default.createTransport({
    service: "gmail",
    host: "smtp.gmail.com",
    port: 587,
    secure: false, // Use `true` for port 465, `false` for all other ports
    auth: {
        user: "aakashsinghrajput.8168@gmail.com",
        pass: "mzrdlpeyuparnjoq",
    },
});
// async..await is not allowed in global scope, must use a wrapper
async function sendGmail(from, to, body, subject) {
    // send mail with defined transport object
    try {
        const info = await transporter.sendMail({
            from: from, // sender address
            to: to, // list of receivers
            subject: subject, // Subject line
            text: body, // plain text body
            html: `<b>${body}</b>`, // html body
        });
        return true;
    }
    catch (error) {
        console.log(error);
        return false;
    }
    // Message sent: <d786aa62-4e0a-070a-47ed-0b0666549519@ethereal.email>
}
exports.sendGmail = sendGmail;
