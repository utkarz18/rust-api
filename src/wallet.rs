use {
    super::schema::wallets,
    crate::miner::{Miner, MinerDAO},
    crate::DBPooledConnection,
    diesel::query_dsl::methods::FilterDsl,
    diesel::result::Error,
    diesel::{ExpressionMethods, Insertable, Queryable, RunQueryDsl},
    serde::{Deserialize, Serialize},
    uuid::Uuid,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct Wallet {
    pub address: String,
    pub club_name: String,
    pub total_hash_rate: i32,
    pub total_shares_mined: i32,
    pub total_workers_online: i32,
    pub workers_online: Vec<Miner>,
}

impl Wallet {
    pub fn to_wallet_dao(&self) -> WalletDao {
        WalletDao {
            address: Uuid::parse_str(self.address.as_str()).unwrap(),
            club_name: self.club_name.to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WalletRequest {
    club_name: String,
}

#[derive(Queryable, Insertable)]
#[table_name = "wallets"]
pub struct WalletDao {
    pub address: Uuid,
    pub club_name: String,
}

impl WalletDao {
    pub fn to_wallet(&self, workers_online: Vec<Miner>) -> Wallet {
        Wallet {
            address: self.address.to_string(),
            club_name: self.club_name.to_string(),
            total_hash_rate: workers_online.iter().map(|w: &Miner| w.hash_rate).sum(),
            total_shares_mined: workers_online.iter().map(|w: &Miner| w.shares_mined).sum(),
            total_workers_online: workers_online.len() as i32,
            workers_online,
        }
    }
}

pub fn fetch_all_wallets(conn: &DBPooledConnection) -> Vec<Wallet> {
    use crate::schema::miners::dsl::*;
    use crate::schema::wallets::dsl::*;
    let all_wallets = match wallets.load::<WalletDao>(conn) {
        Ok(result) => result,
        Err(_) => vec![],
    };
    let all_miners = match miners.load::<MinerDAO>(conn) {
        Ok(result) => result,
        Err(_) => vec![],
    };
    all_wallets
        .into_iter()
        .map(|w| {
            let mut workers_online = vec![];
            for m in all_miners.iter() {
                if m.address.eq(&w.address) {
                    workers_online.push(m.to_miner(w.club_name.clone()));
                };
            }
            w.to_wallet(workers_online)
        })
        .collect::<Vec<Wallet>>()
}

pub fn fetch_wallet_by_id(_address: Uuid, conn: &DBPooledConnection) -> Option<Wallet> {
    use crate::schema::miners::dsl::*;
    use crate::schema::wallets::dsl::*;
    match wallets
        .filter(crate::schema::wallets::address.eq(_address))
        .load::<WalletDao>(conn)
    {
        Ok(result) => match result.first() {
            Some(matched_wallet) => {
                match miners
                    .filter(crate::schema::miners::address.eq(_address))
                    .load::<MinerDAO>(conn)
                {
                    Ok(miner_result) => Some(
                        matched_wallet.to_wallet(
                            miner_result
                                .into_iter()
                                .map(|m| m.to_miner(matched_wallet.club_name.clone()))
                                .collect::<Vec<Miner>>(),
                        ),
                    ),
                    Err(_) => Some(matched_wallet.to_wallet(vec![])),
                }
            }
            _ => None,
        },
        Err(_) => None,
    }
}

pub fn create_new_wallet(
    wallet_request: WalletRequest,
    conn: &DBPooledConnection,
) -> Result<Wallet, Error> {
    use crate::schema::wallets::dsl::*;
    let new_wallet = WalletDao {
        address: Uuid::new_v4(),
        club_name: wallet_request.club_name,
    };

    match diesel::insert_into(wallets)
        .values(&new_wallet)
        .execute(conn)
    {
        Ok(_) => Ok(new_wallet.to_wallet(vec![])),
        Err(e) => Err(e),
    }
}
