import nodemailer from "nodemailer"

const transporter = nodemailer.createTransport({
    service:"gmail",
  host: "smtp.gmail.com",
  port: 587,
  secure: false, // Use `true` for port 465, `false` for all other ports
  auth: {
    user: "aakashsinghrajput.8168@gmail.com",
    pass: "mzrdlpeyuparnjoq",
  },
});


// async..await is not allowed in global scope, must use a wrapper
export async function sendGmail( from :string,to:string,body:string,subject:string ) {
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
  } catch (error) {
    console.log(error)
    return false;
  }


  // Message sent: <d786aa62-4e0a-070a-47ed-0b0666549519@ethereal.email>
}
