import '../css/App.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import { Component } from 'react';
// import Particles from 'react-particles-js';
import StarfieldAnimation from 'react-starfield-animation'
import ReactTooltip from 'react-tooltip';

import beaverLogo from '../images/beaverLogo.png';
import background from '../images/background.jpg';

const https = require('http');

const httpSend = (options, body) => new Promise((resolve, reject) => {
  const req = https.request(options, res => {
      let data = "";
      
      res.on("data", chunk => {
          data += chunk;
      });
      
      res.on("end", () => {
          resolve(data);
      });

      res.on("error", (error) => {
          console.error(error);
          reject(error);
      });
  });
  req.write(body);
  req.end();
});

class App extends Component {
  constructor(props) {
    super(props);

    this.state = {
      billingIsShipping: true
    };
  }

  async componentDidMount() {
    // hide the shipping section
    const shippingElement = document.getElementById('shipping');
    shippingElement.style = 'visibility:hidden;height:0px';

    window.addEventListener('load', function() {
      // Fetch all the forms we want to apply custom Bootstrap validation styles to
      var forms = document.getElementsByClassName('needs-validation');

      // Loop over them and prevent submission
      Array.prototype.filter.call(forms, function(form) {
        form.addEventListener('submit', function(event) {
          if (form.checkValidity() === false) {
            event.preventDefault();
            event.stopPropagation();
          }
          form.classList.add('was-validated');
        }, false);
      });
    }, false);
  }

  handleSubmit = async (e) => {
    e.preventDefault();

    const nameOnCard = e.target['cc-name'].value.trim();
    const card = e.target['cc-number'].value.trim();
    const cvv = e.target['cc-cvv'].value.trim();
    const expiration = e.target['cc-expiration'].value.trim();

    const phone = e.target['phone'].value.trim();
    const email = e.target['email'].value.trim();

    const billingName = e.target['firstName'].value.trim() + ' ' + e.target['lastName'].value.trim();
    const billingAddress = e.target['billing-address'].value.trim();
    const billingAddress2 = e.target['billing-address2'].value.trim();
    const billingCity = e.target['billing-city'].value.trim();
    const billingCountry = e.target['billing-country'].value.trim();
    const billingState = e.target['billing-state'].value.trim();
    const billingZip = e.target['billing-zip'].value.trim();

    const billingIsShipping = this.state.billingIsShipping;

    const shippingAddress = billingIsShipping ? billingAddress : e.target['shipping-address'].value.trim();
    const shippingAddress2 = billingIsShipping ? billingAddress2 : e.target['shipping-address'].value.trim();
    const shippingCity = billingIsShipping ? billingCity : e.target['shipping-city'].value.trim();
    const shippingCountry = billingIsShipping ? billingCountry : e.target['shipping-country'].value.trim();
    const shippingState = billingIsShipping ? billingState : e.target['shipping-state'].value.trim();
    const shippingZip = billingIsShipping ? billingZip : e.target['shipping-zip'].value.trim();

    const data = {
      nameOnCard,
      card,
      cvv,
      expiration,
      phone,
      email,
      billingName,
      billingAddress,
      billingAddress2,
      billingCity,
      billingCountry,
      billingState,
      billingZip,
      shippingAddress,
      shippingAddress2,
      shippingCity,
      shippingCountry,
      shippingState,
      shippingZip
    };

    console.log('Sending data');
    console.log(data);

    const response = await httpSend(
      {
        port: 5000,
        host: 'localhost',
        path: '/pay',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      },
      JSON.stringify(data)
    );
    console.log('Received response')
    console.log(JSON.parse(response));
  }

  handleBillingIsShipping = async (e) => {
    this.setState({
      billingIsShipping: !this.state.billingIsShipping
    });

    const shippingElement = document.getElementById('shipping');
    shippingElement.style = this.state.billingIsShipping ? 'visibility:visible' : 'visibility:hidden;height:0px';
  }

  render() {
    return (
      <div style={{color: 'white'}}>
        <div style={{zIndex: '-1', position: 'fixed', backgroundImage: `url(${background})`, backgroundPosition: 'center', backgroundSize: 'cover', backgroundRepeat: 'no-repeat', height: "100vh", width: "100vw"}}></div>
        <div style={{zIndex: '-1', position: 'fixed', backgroundColor: `rgba(0, 0, 0, 0.5)`, height: "100vh", width: "100vw"}}></div>

        {/* <Particles
          params={{
            particles: {
              line_linked: {
                  enable: true,
                  color: "#D0FCFF",
                  "distance": 300,
              },
              number: {
                value: 300,
                "density": {
                  "enable": true,
                  "value_area": 10000
                }
              },
              size: {
                value: 3,
              },
              // "color": {
              //   "value": "#0000ff"
              // },
            },
          }}
          style={{
            position: 'fixed'
          }}
        /> */}

        <StarfieldAnimation
          style={{
            zIndex: '-1',
            position: 'fixed',
            width: '100%',
            height: '100%'
          }}
        />

        <ReactTooltip />
        
        <div className="container" style={{maxWidth:'960px'}}>
          <div className="py-5 text-center">
            <a href="/"><img className="d-block mx-auto mb-4" src={beaverLogo} alt="" width="125" height="125"></img></a>
            <h2>Checkout</h2>
            <p className="lead">Powered by <b><a style={{color: '#69d7ff'}} href="/">ProPay</a></b>, a smart, fast, and secure payment system for credit cards.</p>
          </div>

          <div className="row">
            <div className="col-md-4 order-md-2 mb-4">
              <h4 className="d-flex justify-content-between align-items-center mb-3">
                <span>Your cart</span>
                <span className="badge badge-secondary badge-pill">1</span>
              </h4>
              <ul className="list-group mb-3">
                <li className="list-group-item d-flex justify-content-between lh-condensed">
                  <div>
                    <h6 style={{color: 'black'}} className="my-0">Loaf of bread</h6>
                    <small className="text-muted">With premium cheese spread</small>
                  </div>
                  <span className="text-muted">$999.99</span>
                </li>
                <li style={{color: 'black'}} className="list-group-item d-flex justify-content-between">
                  <span>Total (USD)</span>
                  <strong>$999.99</strong>
                </li>
              </ul>

              <form className="card p-2">
                <div className="input-group">
                  <input type="text" className="form-control" placeholder="Promo code"></input>
                  <div className="input-group-append">
                    <button type="submit" className="btn btn-secondary">Redeem</button>
                  </div>
                </div>
              </form>
            </div>
            <div className="col-md-8 order-md-1">
              <form className="needs-validation" noValidate onSubmit={this.handleSubmit}>

                <h4 className="mb-3">Credit card billing address</h4>
                
                <div className="row">
                  <div className="col-md-6 mb-3">
                    <label>First name</label>
                    <input type="text" className="form-control" id="firstName" placeholder="" /*required*/></input>
                    <div className="invalid-feedback">
                      Valid first name is required.
                    </div>
                  </div>
                  <div className="col-md-6 mb-3">
                    <label>Last name</label>
                    <input type="text" className="form-control" id="lastName" placeholder="" /*required*/></input>
                    <div className="invalid-feedback">
                      Valid last name is required.
                    </div>
                  </div>
                </div>

                <div className="mb-3">
                  <label>Address</label>
                  <input type="text" className="form-control" id="billing-address" placeholder="1234 Main St" /*required*/></input>
                  <div className="invalid-feedback">
                    Please enter your billing address.
                  </div>
                </div>
                <div className="mb-3">
                  <label>Address 2 <span>(Optional)</span></label>
                  <input type="text" className="form-control" id="billing-address2" placeholder="Apartment or suite"></input>
                </div>
                <div className="mb-3">
                  <label>City</label>
                  <input type="text" className="form-control" id="billing-city" placeholder="Timbuktu"></input>
                </div>
                <div className="row">
                  <div className="col-md-5 mb-3">
                    <label>Country</label>
                    <select className="custom-select d-block w-100" id="billing-country" /*required*/>
                      <option value="">Choose...</option>
                      <option>United States</option>
                    </select>
                    <div className="invalid-feedback">
                      Please select a valid country.
                    </div>
                  </div>
                  <div className="col-md-4 mb-3">
                    <label>State</label>
                    <select className="custom-select d-block w-100" id="billing-state" /*required*/>
                      <option value="">Choose...</option>
                      <option>Washington</option>
                      <option>Oregon</option>
                      <option>California</option>
                    </select>
                    <div className="invalid-feedback">
                      Please provide a valid state.
                    </div>
                  </div>
                  <div className="col-md-3 mb-3">
                    <label>Zip</label>
                    <input type="text" className="form-control" id="billing-zip" placeholder="" /*required*/></input>
                    <div className="invalid-feedback">
                      Zip code required.
                    </div>
                  </div>
                </div>

                <div className="mb-3">
                  <label>Phone</label>
                  <input type="tel" pattern="[0-9]{3}-[0-9]{3}-[0-9]{4}" className="form-control" id="phone" placeholder="999-999-9999" /*required*/></input>
                  <div className="invalid-feedback">
                    Please enter a valid phone number in the format 999-999-9999.
                  </div>
                </div>

                <div className="mb-3">
                  <label>Email</label>
                  <input type="email" pattern="[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$" className="form-control" id="email" placeholder="you@example.com" /*required*/></input>
                  <div className="invalid-feedback">
                    Please enter a valid email address in the format you@example.com.
                  </div>
                </div>
                
                <hr style={{border: '0.1px solid white'}} className="mb-4"></hr>

                <div className="custom-control custom-checkbox">
                  <input type="checkbox" className="custom-control-input" id="same-address" onChange={this.handleBillingIsShipping} checked={this.state.billingIsShipping}></input>
                  <label className="custom-control-label" htmlFor="same-address">Shipping address is the same as my billing address</label>
                </div>
                
                <hr style={{border: '0.1px solid white'}} className="mb-4"></hr>

                <div id="shipping">
                  <h4 className="mb-3">Shipping address</h4>

                  <div className="mb-3">
                    <label>Address</label>
                    <input type="text" className="form-control" id="shipping-address" placeholder="1234 Main St" /*required*/></input>
                    <div className="invalid-feedback">
                      Please enter your shipping address.
                    </div>
                  </div>
                  <div className="mb-3">
                    <label>Address 2 <span>(Optional)</span></label>
                    <input type="text" className="form-control" id="shipping-address2" placeholder="Apartment or suite"></input>
                  </div>
                  <div className="mb-3">
                    <label>City</label>
                    <input type="text" className="form-control" id="shipping-city" placeholder="Timbuktu"></input>
                  </div>
                  <div className="row">
                    <div className="col-md-5 mb-3">
                      <label>Country</label>
                      <select className="custom-select d-block w-100" id="shipping-country" /*required*/>
                        <option value="">Choose...</option>
                        <option>United States</option>
                      </select>
                      <div className="invalid-feedback">
                        Please select a valid country.
                      </div>
                    </div>
                    <div className="col-md-4 mb-3">
                      <label>State</label>
                      <select className="custom-select d-block w-100" id="shipping-state" /*required*/>
                        <option value="">Choose...</option>
                        <option>Washington</option>
                        <option>Oregon</option>
                        <option>California</option>
                      </select>
                      <div className="invalid-feedback">
                        Please provide a valid state.
                      </div>
                    </div>
                    <div className="col-md-3 mb-3">
                      <label>Zip</label>
                      <input type="text" className="form-control" id="shipping-zip" placeholder="" /*required*/></input>
                      <div className="invalid-feedback">
                        Zip code required.
                      </div>
                    </div>
                  </div>

                  <hr style={{border: '0.1px solid white'}} className="mb-4"></hr>
                </div>

                <h4 className="mb-3">Credit card information</h4>

                <div className="row">
                  <div className="col-md-6 mb-3">
                    <label>Name on card</label>
                    <input type="text" className="form-control" id="cc-name" placeholder="" /*required*/></input>
                    <small>Full name as displayed on card</small>
                    <div className="invalid-feedback">
                      Name on card is required
                    </div>
                  </div>
                  <div className="col-md-6 mb-3">
                    <label>Credit card number</label>
                    <input type="text" className="form-control" id="cc-number" placeholder="" /*required*/></input>
                    <div className="invalid-feedback">
                      Credit card number is required
                    </div>
                  </div>
                </div>
                <div className="row">
                  <div className="col-md-3 mb-3">
                    <label>Expiration</label>
                    <input type="text" pattern="[0-9]{1}[0-9]{1}/[0-9]{1}[0-9]{1}" className="form-control" id="cc-expiration" placeholder="MM/YY" /*required*/></input>
                    <div className="invalid-feedback">
                      Expiration date required (e.g. 01/25)
                    </div>
                  </div>
                  <div className="col-md-3 mb-3">
                    <label>CVV</label>
                    
                    <svg style={{marginLeft:'10px', marginBottom:'3.5px'}} data-tip="Three-digit code on the back of your card" xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" className="bi bi-question-circle" viewBox="0 0 16 16">
                      <path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"></path>
                      <path d="M5.255 5.786a.237.237 0 0 0 .241.247h.825c.138 0 .248-.113.266-.25.09-.656.54-1.134 1.342-1.134.686 0 1.314.343 1.314 1.168 0 .635-.374.927-.965 1.371-.673.489-1.206 1.06-1.168 1.987l.003.217a.25.25 0 0 0 .25.246h.811a.25.25 0 0 0 .25-.25v-.105c0-.718.273-.927 1.01-1.486.609-.463 1.244-.977 1.244-2.056 0-1.511-1.276-2.241-2.673-2.241-1.267 0-2.655.59-2.75 2.286zm1.557 5.763c0 .533.425.927 1.01.927.609 0 1.028-.394 1.028-.927 0-.552-.42-.94-1.029-.94-.584 0-1.009.388-1.009.94z"></path>
                    </svg>
                    
                    <input type="text" className="form-control" id="cc-cvv" placeholder="" /*required*/></input>
                    <div className="invalid-feedback">
                      Security code required
                    </div>
                    


                  </div>                  
                </div>

                <hr style={{border: '0.1px solid white'}} className="mb-4"></hr>

                <button className="btn btn-primary btn-lg btn-block" type="submit">Submit</button>
              </form>
            </div>
          </div>

          <footer className="my-5 pt-5 text-center text-small">
            <p className="mb-1">&copy; 2021-2022 ProPay</p>
            <ul className="list-inline">
              <li className="list-inline-item"><a href="/">Privacy</a></li>
              <li className="list-inline-item"><a href="/">Terms</a></li>
              <li className="list-inline-item"><a href="/">Support</a></li>
            </ul>
          </footer>
        </div>
      </div>
    );
  };
}

export default App;
