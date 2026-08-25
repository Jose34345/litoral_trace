# Smart Import — retorno seguro bajo FORCE RLS

`SmartImportProfileService.remember()` no realiza un `refresh()` posterior al `commit` sobre `smart_import_profiles`.

El contexto `app.current_organization_id` se instala de forma transaccional en PostgreSQL. Por lo tanto, después del `commit` ese contexto deja de existir y una lectura posterior sobre una tabla con `FORCE ROW LEVEL SECURITY` no debe ejecutarse sin reinstalar explícitamente el tenant.

El contrato adoptado es fail-closed:

- el perfil y su evento de auditoría se escriben dentro de la misma transacción;
- ambos se fuerzan con `flush()` antes de confirmar;
- la instancia del perfil se desacopla de la sesión antes del `commit`;
- no se abre una lectura posterior al `commit` sólo para materializar el valor de retorno;
- cualquier fallo previo al `commit` provoca rollback de la mutación y de su auditoría.

Existe cobertura de regresión específica para impedir que reaparezca un `session.refresh(profile)` posterior al `commit` sin contexto tenant.
