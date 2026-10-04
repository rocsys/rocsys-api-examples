const API_SECRET = 'TOPSECRET'

const express = require('express')
const app = express()
app.use(express.json())
const port = 9000

const handleWebhook = (req, res) => {
  console.log('------ Received a webhook message');
  if (req.headers['authorization'] !== `Bearer ${API_SECRET}`) {
    console.log('Unauthorized')
    res.status(401).send('Unauthorized')
    return
  }
  console.log('Authorization OK');
  if (req.headers['x-rocsys-api-type']) {
    console.log(`Got a ${req.headers['x-rocsys-api-type']} message from Rocsys API:`);
  }
  console.log('------ Done processing the webhook message');
  console.log(req.body);
  res.send('ok');
}

app.post('/webhook', handleWebhook)
app.listen(port, () => console.log(`Web-hook listener app listening on port ${port}!`))
