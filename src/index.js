const { app } = require('@azure/functions');

app.setup({
    enableHttpStream: true,
});

require('./functions/dbModifiers/postStatus');
require('./functions/dbModifiers/addSensor');
require('./functions/dbModifiers/addClient');
require('./functions/dbModifiers/addParking');
require('./functions/dbModifiers/addLevel');
require('./functions/dbModifiers/addUser');
require('./functions/dbModifiers/grantPermission');
require('./functions/dbModifiers/updateFlags');
require('./functions/dbModifiers/updateMaintenance');

require('./functions/dbFetchers/getGeneralInfo');
require('./functions/dbFetchers/getLevelsByUser');
require('./functions/dbFetchers/getSensorsByLevel');
require('./functions/dbFetchers/getValidDates');
require('./functions/dbFetchers/getSensorsByUser');

require('./functions/dbFetchers/getAnalysis');
require('./functions/dbFetchers/getStatsByDateBucket');