use ella_tensor as tensor;
use tensor::Tensor;

fn main() {
    let x = Tensor::linspace(0_f32, 10., 100).cos();
    let y = Tensor::linspace(0_f32, 10., 100).sin();

    let mask = Tensor::range(0_i32, 100, 1) % 2;
    let y = y.with_mask(mask.eq(0));
    let z = x.unsqueeze(0) * y.unsqueeze(-1);
    println!("{:?}", z);

    let s1 = tensor::tensor!["A".to_string(), "B".to_string(), "C".to_string()];
    let s2 = tensor::tensor![Some("A".to_string()), None, Some("B".to_string())];
    println!("{:?}", s1);
    println!("{:?}", s1.eq(s2));
}
