const fs = require('fs');

const MAX_CARDS = 1000;

main();

async function main(){
    let csvTxt = 'nameOnCard,card,cvv,expiration,phone,email,billingName,billingAddress,billingAddress2,billingCity,billingCountry,billingState,billingZip,shippingAddress,shippingAddress2,shippingCity,shippingCountry,shippingState,shippingZip\n';

    for(let i=0; i<MAX_CARDS; i++) {
        const zip = createFixedLengthNumber(999999);

        const nameOnCard = `User-${i}`;
        const card = createFixedLengthNumber(9999999999999999);
        const cvv = createFixedLengthNumber(999);
        const expiration = `${createFixedLengthNumber(12)}/${createFixedLengthNumber(30)}`;
        const phone = createFixedLengthNumber(9999999999);
        const email = `User-${i}@test.com`;
        const billingName = `User-${i}`;
        const billingAddress = `${i} Eagle St`;
        const billingAddress2 = ``;
        const billingCity = `Seattle`;
        const billingCountry = `United States`;
        const billingState = `Washington`;
        const billingZip = zip;
        const shippingAddress = `${i} Eagle St`;
        const shippingAddress2 = ``;
        const shippingCity = `Seattle`;
        const shippingCountry = `United States`;
        const shippingState = `Washington`;
        const shippingZip = zip;

        csvTxt += `${nameOnCard},${card},${cvv},${expiration},${phone},${email},${billingName},${billingAddress},${billingCity},${billingAddress2},${billingCountry},${billingState},${billingZip},${shippingAddress},${shippingAddress2},${shippingCity},${shippingCountry},${shippingState},${shippingZip}\n`;
    }

    // console.log(csvTxt);

    await writeFile('./test-data.csv', csvTxt);
}

function createFixedLengthNumber(max){ // specify the max number (e.g. length 4 number: 9999)
    const maxm = max;
    const minm = 0;
    return Math.floor(Math.random() * (maxm - minm + 1)) + minm;
}

async function writeFile(file, data){
    return new Promise((resolve, reject) => {
        fs.writeFile(file, data, (err) => {
            if(err){
                console.log(err);
                reject(err);
            } else {
                console.log(`Succesfully wrote data to ${file}`);
                resolve();
            }
        });
    });
}