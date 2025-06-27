const { app } = require('@azure/functions');

app.setup({
    enableHttpStream: true,
});

require('./functions/dbModifiers/insertSensor');
require('./functions/dbModifiers/postStatus');
require('./functions/dbModifiers/addClient');
