use synapse_tensor as tensor;
use tensor::Tensor;

fn main() {
    let x = Tensor::linspace(0_f32, 10., 100).cos();
    let y = Tensor::linspace(0_f32, 10., 100).sin();

    let mask = Tensor::range(0_i32, 100, 1) % 2;
    let y = y.with_mask(mask.eq(0));
    let z = x.unsqueeze(0) * y.unsqueeze(-1);
    println!("{:?}", z);
}
