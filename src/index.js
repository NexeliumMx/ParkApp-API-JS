const { app } = require('@azure/functions');

app.setup({
    enableHttpStream: true,
});

require('./functions/dbModifiers/addSensor');
require('./functions/dbModifiers/postStatus');
require('./functions/dbModifiers/addClient');
require('./functions/dbModifiers/addParking');
require('./functions/dbModifiers/addLevel');
