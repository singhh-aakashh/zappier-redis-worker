const  bcrypt = require('bcryptjs');

const compare = bcrypt.compare("aakash","$2a$10$/zx5bNPxrkDz5luieHRZM.6nlnNoPiWi4xSwbUsEwuWMoOQXdq8eG").then(
    res => console.log(res)
)

// $2a$10$/zx5bNPxrkDz5luieHRZM.6nlnNoPiWi4xSwbUsEwuWMoOQXdq8eG