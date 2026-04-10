CREATE FUNCTION introspect.set_default_timestamps() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.__created_at IS NULL THEN
        NEW.__created_at := NOW();
    END IF;
    IF NEW.__updated_at IS NULL THEN
        NEW.__updated_at := NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;