from pathlib import Path

from shared.migration_cutover import START,END


def test_cutover_range():
    assert (START,END)==(1501,2000)


def test_cutover_order_contract():
    s=Path('shared/migration_cutover.py').read_text(encoding='utf-8')
    begin=s.index('begin_fencing(meta_dsn,job_id)',s.index('def cutover_once'))
    fence=s.index('fence_source(source_dsn',begin)
    replay=s.index('final_sync(source_dsn',fence)
    map_=s.index('activate_map(meta_dsn',replay)
    target=s.index('activate_target(target_dsn',map_)
    assert begin<fence<replay<map_<target


def test_metadata_epoch_is_published_in_same_transaction_as_map():
    s=Path('shared/migration_cutover.py').read_text(encoding='utf-8')
    f=s[s.index('def activate_map'):s.index('def activate_target')]
    assert 'with db.transaction()' in f
    assert 'UPDATE shard_map' in f and 'INSERT INTO shard_map' in f
    assert 'UPDATE routing_epoch' in f


def test_replica_mutations_lock_local_fences():
    s=Path('shared/local_bank.py').read_text(encoding='utf-8')
    assert 'FOR SHARE' in s
    assert 'require_local_fences(conn, accounts, shard, epoch)' in s
    p=Path('shared/participant_2pc.py').read_text(encoding='utf-8')
    assert 'require_local_fences(conn' in p
