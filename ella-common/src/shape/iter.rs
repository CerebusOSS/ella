use super::Shape;

#[derive(Debug)]
pub struct ShapeIndexIter<S> {
    pub(crate) shape: S,
    pub(crate) index: Option<S>,
}

impl<S: Shape> ShapeIndexIter<S> {
    pub(crate) fn new(shape: S) -> Self {
        let index = Self::first_index(&shape);
        Self { shape, index }
    }

    pub(crate) fn first_index(shape: &S) -> Option<S> {
        if shape.size() == 0 {
            None
        } else {
            Some(S::zeros(shape.ndim()))
        }
    }

    pub(crate) fn shape_next(shape: &S, mut index: S) -> Option<S> {
        for (&d, i) in shape.slice().iter().zip(index.as_mut()).rev() {
            *i += 1;
            if *i == d {
                *i = 0;
            } else {
                return Some(index);
            }
        }
        None
    }
}

impl<S: Shape> Iterator for ShapeIndexIter<S> {
    type Item = S;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index.as_ref()?.clone();
        self.index = Self::shape_next(&self.shape, index.clone());
        Some(index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<S: Shape> ExactSizeIterator for ShapeIndexIter<S> {
    fn len(&self) -> usize {
        match self.index {
            Some(ref idx) => {
                let consumed = self
                    .shape
                    .default_strides()
                    .slice()
                    .iter()
                    .zip(idx.slice())
                    .fold(0, |s, (&a, &b)| s + a * b);
                self.shape.size() - consumed
            }
            None => 0,
        }
    }
}
