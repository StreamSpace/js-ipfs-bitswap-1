let x = process.argv.slice(2)
console.log(x[0])
const fs = require('fs');

process.stdin.resume();
process.stdin.setEncoding('ascii');

var input_stdin = "";
process.stdin.on('data', function (data) {
    input_stdin += data;
});

process.stdin.on('end', function () {
   fptr = fs.createWriteStream(process.env['OUTPUT_FILE_PATH']);
   fptr.write("\n");
   inputs = input_stdin.split('\n');
   var iterator = 0;
       a = parseInt(inputs[iterator++].trim());
       b = parseInt(inputs[iterator++].trim());

   outcome = summation(a,b);

   fptr.write(outcome  + '\n');

   fptr.end();

});

// implement method/function with name 'summation' below.
//
// The function accepts following parameters:
//  1. a is of type INTEGER.
//  2. b is of type INTEGER.


function summation(a,b) {
    // Write your code here

    return;
}