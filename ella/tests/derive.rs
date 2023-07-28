// use ella::RowFormat;

// #[derive(Debug, Clone, RowFormat)]
// #[row(view = "SimpleView", builder = "SimpleBuilder")]
// struct Simple {
//     x: f32,
//     y: i32,
// }

// #[derive(Debug, Clone, RowFormat)]
// struct Tuple(
//     #[row(name = "x")] u8,
//     #[row(name = "y")] ella::Tensor2<f64>,
// );

// #[derive(Debug, Clone, RowFormat)]
// struct AsType {
//     #[row(type = "ella::Tensor1<f32>")]
//     z: Vec<f32>,
// }

// #[derive(Debug, Clone, RowFormat)]
// struct Empty {}

// #[derive(Debug, Clone, RowFormat)]
// struct NewType(Simple);
