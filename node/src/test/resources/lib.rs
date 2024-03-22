use we_contract_sdk::*;

// Smart contract used in WASMServiceSpec.scala

// Declaring an interface to interact with another contract.
// The types used, for specifying the type of arguments,
// are also available for use within the contract.
#[interface]
trait i_contract {
    fn function(
        integer: Integer,
        boolean: Boolean,
        binary: Binary,
        string: String
    );
    fn close();
}

// Declaring a function available for calling.
// `#[action]` keyword is used for this purpose.
// The called function always returns 0 inside itself.
// All available functions have an error handler which,
// in case of an error, exits the function with an error code.
#[action]
fn _constructor() {
    // Converting a string address to a byte address.
    let contract = base58!("4WVhw3QdiinpE5QXDG7QfqLiLanM7ewBw4ChX4qyGjs2");
    let asset = base58!("DnK5Xfi2wXUJx9BjK9X6ZpFdTLdq2GtWH9pWrcxcmrhB");

    // Values that will serve as arguments for the function call.
    // The types used are aliases over Rust types.
    // These types are used in interface declaration.
    let integer: Integer = 42;
    let boolean: Boolean = true;
    let binary: Binary = &[0, 1];
    let string: String = "test";
    let payment: Payment = (asset, 2400000000);

    // Calling a contract.
    // In this case, the `i_contract` interface and the address of the contract to be called are used.
    // And accordingly the function that needs to be called.
    call_contract! {
        i_contract(contract) => function(integer, boolean, binary, string)
    };

    // Converting a string address to a byte address.
    let address = base58!("3NBSHvNfkH4vv4dM5QCxRNBXYt6MFknJNnX");

    let balance = get_balance!(address => address);

    set_storage!(integer => ("balance", balance));

    // Verify inputs and conditions.
    // In this case, a balance check is performed.
    // require!(balance > 0);
    // Transfer of funds to the `address`.
    transfer!(address, 42);
}

#[action]
fn function(
    integer: Integer,
    boolean: Boolean,
    binary: Binary,
    string: String
) {
    set_storage!(integer => ("integer", integer));
    set_storage!(boolean => ("boolean", boolean));
    set_storage!(binary  => ("binary", binary));
    set_storage!(string  => ("string", string));
}

