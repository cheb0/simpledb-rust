use crate::plan::Plan;
use crate::query::Scan;
use crate::query::project_scan::ProjectScan;
use crate::record::schema::Schema;
use crate::tx::Transaction;

pub struct ProjectPlan {
    plan: Box<dyn Plan>,
    schema: Schema,
    fieldlist: Vec<String>,
}

impl ProjectPlan {
    pub fn new(plan: Box<dyn Plan>, fields: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for field in &fields {
            schema.add_from_schema(field, &plan.schema());
        }
        ProjectPlan {
            plan,
            schema,
            fieldlist: fields,
        }
    }
}

impl Plan for ProjectPlan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx> {
        let scan = self.plan.open(tx);
        Box::new(ProjectScan::new(scan, self.fieldlist.clone()))
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
