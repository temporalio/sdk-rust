use std::error::Error;

pub(crate) fn try_into_or_box_err<A, B, E, MapErr>(
    val: Option<A>,
    map_err: MapErr,
) -> Result<Option<B>, E>
where
    A: TryInto<B>,
    <A as TryInto<B>>::Error: Error + Send + Sync + 'static,
    MapErr: FnOnce(Box<dyn Error + Send + Sync + 'static>) -> E,
{
    val.map(TryInto::try_into)
        .transpose()
        .map_err(|e| map_err(Box::from(e)))
}
